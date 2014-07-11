package moosefs

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/golang/glog"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type StatInfo struct {
	Totalspace    uint64
	Availspace    uint64
	Trashspace    uint64
	Reservedspace uint64
	Inodes        uint32
}

type Chunk struct {
	Id      uint64
	Version uint32
	Length  uint64
	Addrs   []string
}

func (c *Chunk) String() string {
	return fmt.Sprintf("<Chunk(%d, %d, %d)>", c.Id, c.Version, c.Length)
}

func NewChunk(id, length uint64, version uint32, csdata []byte) *Chunk {
	var addrs []string
	addrsLen := len(csdata)
	if addrsLen > 0 {
		addrs = make([]string, addrsLen/6)
		idx := 0
		for i := 0; i < addrsLen; i = i + 6 {
			a := Unpack_I(csdata[i : i+4])
			port := Unpack_H(csdata[i+4 : i+6])
			addr := fmt.Sprintf("%d.%d.%d.%d:%d",
				byte(a>>24), byte(a>>16), byte(a>>8), byte(a), port)
			addrs[idx] = addr
		}
	}
	return &Chunk{
		Id:      id,
		Length:  length,
		Version: version,
		Addrs:   addrs,
	}
}

type LockReply struct {
	sync.RWMutex
	m map[int]*resultItem
}

type MasterConn struct {
	host       string
	port       int32
	uid        uint32
	gid        uint32
	sessionid  uint32
	packetid   chan uint32
	fail_count int
	dcache     map[uint32]map[string]*FileInfo
	dstat      map[uint32]uint32

	reply []*resultItem //TODO: use dic to make when concurrent
	lock  *sync.RWMutex

	connLock *sync.Mutex
	conn     *net.TCPConn

	IsReady bool
}

type resultItem struct {
	d    []byte
	data []byte
	err  error
}

func NewMasterConn(host string, port int32) *MasterConn {
	m := &MasterConn{}
	m.host = host
	m.port = port
	m.uid = uint32(os.Getuid())
	m.gid = uint32(os.Getgid())

	var packetId uint32 = 0
	p := make(chan uint32)
	go func() {
		for {
			p <- atomic.AddUint32(&packetId, 1)
		}
	}()
	m.packetid = p

	m.lock = new(sync.RWMutex)
	m.connLock = new(sync.Mutex)

	go m.heartbeat()
	go m.recv_thread()
	return m
}

func (m *MasterConn) heartbeat() {
	for {
		if err := m.nop(); err != nil {
			m.Close()
		}
		time.Sleep(2 * time.Second)
	}
}

func (m *MasterConn) Connect() error {
	m.connLock.Lock()
	defer m.connLock.Unlock()
	if m.conn != nil { //already connected
		return nil
	}
	addr := fmt.Sprintf("%s:%d", m.host, m.port)
	tcpAddr, err := net.ResolveTCPAddr("tcp4", addr)
	if err != nil {
		return err
	}
	for i := 0; i < 10; i++ {
		conn, err := net.DialTCP("tcp", nil, tcpAddr)
		if err == nil {
			m.conn = conn
			break
		}
		m.fail_count += 1
		time.Sleep(time.Duration(15/10*m.fail_count*m.fail_count) * time.Second)
	}
	if m.conn == nil {
		return errors.New("mfsmaster not available")
	}
	regbuf := Pack(CUTOMA_FUSE_REGISTER, FUSE_REGISTER_BLOB_NOACL,
		m.sessionid, VERSION)
	if err := m.send(regbuf); err != nil {
		return err
	}

	recv, err := m._recv(8)
	if err != nil {
		return err
	}
	cmd, i := Unpack_II(recv)
	if cmd != MATOCU_FUSE_REGISTER {
		return errors.New(fmt.Sprintf("got incorrect answer from mfsmaster %d", cmd))
	}
	if i != 1 && i != 4 {
		return errors.New("got incorrect size from mfsmaster")
	}
	data, err := m._recv(i)
	if err != nil {
		return err
	}
	if i == 1 {
		code := data[0]
		if code != 0 {
			return errors.New(fmt.Sprintf("mfsmaster register error: %d",
				mfs_strerror(int(code))))
		}
	}
	if m.sessionid == 0 {
		m.sessionid = Unpack_I(data)
	}
	m.IsReady = true
	return nil
}

func (m *MasterConn) Close() {
	m.connLock.Lock()
	defer m.connLock.Unlock()
	m._close()
}

//TODO:need to upgrade
func (m *MasterConn) _close() {
	if m.conn != nil {
		m.conn.Close()
		m.conn = nil

		m.dcache = make(map[uint32]map[string]*FileInfo)
		m.IsReady = false
	}
}

func (m *MasterConn) send(buf []byte) error {
	if m.conn == nil {
		return errors.New("not connected")
	}
	n, err := m.conn.Write(buf)
	if err == nil {
		for n < len(buf) {
			sent, err := m.conn.Write(buf[n:])
			if err != nil {
				m._close()
				return errors.New("write to master failed")
			}
			n += sent
		}
	} else {
		m._close()
	}
	return err
}

func (m *MasterConn) nop() error {
	if err := m.Connect(); err != nil {
		return err
	}
	msg := Pack(ANTOAN_NOP, 0)
	if err := m.send(msg); err != nil {
		return err
	}
	return nil
}

func (m *MasterConn) _recv(usize uint32) ([]byte, error) {
	var err error
	buf := new(bytes.Buffer)
	if m.conn == nil {
		return nil, errors.New("not connected")
	}
	tmp := make([]byte, usize)
	size := int(usize)
	n, err := m.conn.Read(tmp)
	if err == nil {
		buf.Write(tmp[:n])
		for n < size {
			recvSize, err := m.conn.Read(tmp)
			if err != nil {
				break
			}
			buf.Write(tmp[:recvSize])
			n += recvSize
		}
	}
	return buf.Bytes(), err
}

func (m *MasterConn) recv_cmd() ([]byte, []byte, error) {
	var (
		d    []byte
		data []byte
		err  error
		cmd  uint32
		size uint32
	)
	d, err = m._recv(12)
	if err != nil {
		return nil, nil, err
	}
	cmd, size = Unpack_II(d)
	if size > 4 {
		data, err = m._recv(size - 4)
		if err != nil {
			return nil, nil, err
		}
	}

	for cmd == ANTOAN_NOP || cmd == MATOCU_FUSE_NOTIFY_DIR || cmd == MATOCU_FUSE_NOTIFY_ATTR {
		switch cmd {
		case ANTOAN_NOP:
		case MATOCU_FUSE_NOTIFY_ATTR:
			for len(data) >= 43 {
				//parent, inode := Unpack_II(data)
				//attr := data[8:43]
				/*
				   caches, ok := m.dcache[parent]
				   if ok {
				       for cache := range caches {
				              if cache.inode == inode {
				                  cache = attrToFileInfo(inode, attr)
				              }
				       }
				   }
				*/
				data = data[43:]
			}
		case MATOCU_FUSE_NOTIFY_DIR:
			for len(data) >= 4 {
				//inode := Unpack_I(data)
				/*
				   caches, ok := m.dcache[inode]
				   if ok {
				       delete(m.dcache, inode)
				       for cache := range caches {
				           if cache.inode == inode {
				               cache = attrToFileInfo(inode, attr)
				           }
				       }
				   }
				*/
				data = data[4:]
			}
		}
		d, err = m._recv(12)
		if err != nil {
			return nil, nil, err
		}
		cmd, size = Unpack_II(d)
		if size > 4 {
			data, err = m._recv(size - 4)
			if err != nil {
				return nil, nil, err
			}
		}
	}
	return d, data, nil
}

func (m *MasterConn) recv_thread() {
	for {
		if !m.IsReady {
			time.Sleep(10 * time.Millisecond)
		} else {
			d, data, err := m.recv_cmd()
			result := resultItem{}
			result.data = data
			result.d = d
			result.err = err
			m.lock.Lock()
			m.reply = []*resultItem{&result}
			m.lock.Unlock()
		}
	}
}

func (m *MasterConn) sendIt(cmd uint32, args ...interface{}) ([]byte, error) {
	packetid := <-m.packetid
	err := m.Connect()
	if err != nil {
		return nil, err
	}
	info := Pack(cmd, packetid, args)
	m.lock.Lock()
	m.reply = nil
	m.lock.Unlock()
	m.send(info)
	totalTime := time.Nanosecond
	timeout := 10 * time.Second
	for {
		var result *resultItem
		m.lock.RLock()
		length := len(m.reply)
		if length > 0 {
			result = m.reply[0]
		}
		m.lock.RUnlock()
		if result == nil {
			sleepTime := 10 * time.Millisecond
			time.Sleep(sleepTime)
			totalTime += sleepTime
			if totalTime > timeout {
				return nil, errors.New("read from master timeout 5s")
			}
		} else {
			h := result.d
			data := result.data
			rcmd, size, pid := Unpack_III(h)
			if rcmd != cmd+1 || pid != packetid || size <= 4 {
				m._close()
				return nil, errors.New(
					fmt.Sprintf("incorrect answer (%d!=%d, %d!=%d, %d<=4",
						rcmd, cmd+1, pid, packetid, size))
			}
			if len(data) == 1 && data[0] != 0 {
				return nil, errors.New(mfs_strerror(int(data[0])))
			}
			return data, nil
		}
	}
}
func (m *MasterConn) sendAndReceive(cmd uint32, args ...interface{}) ([]byte, error) {
	var err error
	for i := 0; i < 4; i++ {
		var data []byte
		data, err = m.sendIt(cmd, args)
		if err == nil {
			return data, err
		}
	}
	if err != nil {
		glog.Warningf("sendAndReceive: %s\n", err)
		return nil, err
	}
	return nil, errors.New("read from master timeout 5 times")
}

func (m *MasterConn) Statfs() (*StatInfo, error) {
	ans, err := m.sendAndReceive(CUTOMA_FUSE_STATFS)
	if err == nil {
		i1, i2, i3, i4, i5 := Unpack_QQQQI(ans)
		return &StatInfo{i1, i2, i3, i4, i5}, nil
	}
	return nil, err
}

func (m *MasterConn) GetDir(inode uint32) (map[string][]uint32, error) {
	//return: {name: (inode,type)}
	ans, err := m.sendAndReceive(CUTOMA_FUSE_GETDIR, inode,
		m.uid, m.gid)
	if err != nil {
		return nil, err
	}
	var names = make(map[string][]uint32)
	var p uint32
	length := uint32(len(ans))
	for p < length {
		size := uint32(Unpack_B(ans[p : p+1]))
		p += 1
		total := size + p + 5
		if total > length {
			break
		}
		name := string(ans[p : p+size])

		p += size
		inode, stype := Unpack_IB(ans)
		itype := uint32(stype)
		names[name] = []uint32{inode, itype}
		p += 5
	}
	return names, nil
}

func (m *MasterConn) GetDirPlus(inode uint32) (map[string]*FileInfo, error) {
	//return {name: FileInfo()}
	if ENABLE_DCACHE {
		if infos, ok := m.dcache[inode]; ok {
			return infos, nil
		}
	}
	flag := GETDIR_FLAG_WITHATTR
	if ENABLE_DCACHE {
		flag |= GETDIR_FLAG_DIRCACHE
	}
	ans, err := m.sendAndReceive(CUTOMA_FUSE_GETDIR, inode,
		m.uid, m.gid, uint8(flag))
	if err != nil {
		return nil, err
	}
	p := 0
	infos := make(map[string]*FileInfo)
	total := len(ans)
	for p < total {
		length := int(Unpack_B(ans[p : p+1]))
		p += 1
		name := string(ans[p : p+length])
		p += length
		i := Unpack_I(ans[p : p+4])
		attr := ans[p+4 : p+39]
		infos[name] = attrToFileInfo(i, attr, name)
		p += 39
	}
	if ENABLE_DCACHE {
		m.dcache[inode] = infos
	}
	return infos, nil
}

// flag defalut = 1
func (m *MasterConn) OpenCheck(inode uint32, flag uint8) ([]byte, error) {
	return m.sendAndReceive(CUTOMA_FUSE_OPEN, inode,
		m.uid, m.gid, uint8(flag))
}

func (m *MasterConn) ReadChunk(inode uint32, index uint32) (*Chunk, error) {
	if ans, err := m.sendAndReceive(CUTOMA_FUSE_READ_CHUNK, inode, index); err == nil {
		n := len(ans)
		if n < 20 || (n-20)%6 != 0 {
			return nil, errors.New(fmt.Sprintf("read chunk: invalid length: %s", n))
		}
		length, id, version := Unpack_QQI(ans)
		return NewChunk(id, length, version, ans[20:]), nil
	} else {
		return nil, err
	}
}

func (m *MasterConn) Lookup(parent uint32, name string) (*FileInfo, error) {
	if ENABLE_DCACHE {
		cache, ok := m.dcache[parent]
		if !ok {
			cache, _ = m.GetDirPlus(parent)
		}
		if val, ok := cache[name]; ok {
			return val, nil
		}
		if val, ok := m.dstat[parent]; ok {
			m.dstat[parent] = val + 1
		} else {
			m.dstat[parent] = 1
		}
	}
	ans, err := m.sendAndReceive(CUTOMA_FUSE_LOOKUP, parent, uint8(len(name)), name, 0, 0)
	if err != nil {
		return nil, err
	}
	if len(ans) == 1 {
		return nil, nil
	}
	if len(ans) != 39 {
		return nil, errors.New("bad length")
	}
	inode := Unpack_I(ans)
	return attrToFileInfo(inode, ans[4:], name), nil
}

func (m *MasterConn) getattr(inode uint32) (*FileInfo, error) {
	ans, err := m.sendAndReceive(CUTOMA_FUSE_GETATTR, inode, m.uid, m.gid)
	if err != nil {
		return nil, err
	}
	return attrToFileInfo(inode, ans, ""), nil
}

func (m *MasterConn) ReadLink(inode uint32) (string, error) {
	ans, err := m.sendAndReceive(CUTOMA_FUSE_READLINK, inode)
	if err != nil {
		return "", err
	}
	length := Unpack_I(ans)
	if length+4 != uint32(len(ans)) {
		return "", errors.New("invalid length")
	}
	return string(ans[4:]), nil
}
