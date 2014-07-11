package gopark

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"github.com/golang/glog"
	"github.com/pwaller/go-clz4"
	"hash/crc32"
	"net"
	"net/rpc"
	"os"
	"strings"
)

var (
	GOB_TYPE uint8 = 0
	//JSON_TYPE   uint8  = 1
	BLOCK_SHIFT uint32 = 20
	BLOCK_SIZE  uint32 = 1 << BLOCK_SHIFT
)

var (
	GUIDE_STOP       = 0
	GUIDE_INFO       = 1
	GUIDE_SOURCES    = 2
	GUIDE_REPORT_BAD = 3
)
var (
	SERVER_STOP       = 0
	SERVER_FETCH      = 1
	SERVER_FETCH_FAIL = 2
	SERVER_FETCH_OK   = 3
)

type BroadcastManager interface {
	Star(is_master bool)
	Shutdown()
	Register(uuid, value string)
	Clear(uuid string)
	Fetch(uuid string, block_num uint64)
}

var _manager *P2PBroadcastManager = newP2PBroadcastManager()

func StartBroadcastManager(is_master bool) {
	_manager.Start(is_master)
}

func StopBroadcastManager() {
	_manager.Shutdown()
}

func pack_BI(b uint8, i uint32) []byte {
	// >BI
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, b)
	binary.Write(buf, binary.BigEndian, i)
	return buf.Bytes()
}

func unpack_BI(buf []byte) (b uint8, i uint32) {
	return buf[0], binary.BigEndian.Uint32(buf[1:5])
}

type BBlock struct {
	Uuid string
	Data interface{}
}

func to_blocks(uuid string, obj interface{}) ([][]byte, error) {
	bType := GOB_TYPE
	var bufs []byte
	var buf bytes.Buffer

	bblock := &BBlock{
		Uuid: uuid,
		Data: obj,
	}

	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(bblock); err != nil {
		return nil, err
	} else {
		bufs = buf.Bytes()
	}
	checksum := crc32.ChecksumIEEE(bufs) & 0xFFFF
	stream := append(pack_BI(bType, checksum), bufs...)
	streamLen := uint32(len(stream))
	blockNum := (streamLen + (BLOCK_SIZE - 1)) >> BLOCK_SHIFT
	blocks := make([][]byte, blockNum)
	var i uint32 = 0
	for i = 0; i < blockNum; i++ {
		right := (i + 1) * BLOCK_SIZE
		if right > streamLen {
			right = streamLen
		}
		input := stream[i*BLOCK_SIZE : right]
		output := []byte{}
		err := clz4.Compress(input, &output)
		if err != nil {
			return nil, err
		}
		blocks[i] = output
	}
	return blocks, nil
}

func from_blocks(uuid string, blocks [][]byte) (interface{}, error) {
	buffs := new(bytes.Buffer)
	for _, block := range blocks {
		decompressed := make([]byte, 0, len(block))
		if err := clz4.UncompressUnknownOutputSize(block, &decompressed); err != nil {
			return nil, err
		} else {
			buffs.Write(decompressed)
		}
	}
	stream := buffs.Bytes()
	bType, checksum := unpack_BI(stream[:5])
	bufs := stream[5:]
	_checksum := crc32.ChecksumIEEE(bufs) & 0xFFFF
	if _checksum != checksum {
		return nil, fmt.Errorf("Wrong blocks: checksum: 0x%08x, expected: 0x%08x", _checksum, checksum)
	}

	val := BBlock{}
	if bType == GOB_TYPE {
		dec := gob.NewDecoder(bytes.NewBuffer(bufs))
		if err := dec.Decode(&val); err != nil {
			return nil, err
		}
	} else {
		return nil, fmt.Errorf("Unknown serialization type: %d", bType)
	}
	_uuid := val.Uuid
	if uuid != _uuid {
		return nil, fmt.Errorf("Wrong blocks: uuid: %s, expected: %s", _uuid, uuid)
	}
	return val.Data, nil
}

type P2PBroadcastManager struct {
	published     map[string][][]byte
	cache         *Cache
	host          string
	guides        map[string]map[string][]int // map[uuid] map[server] [1,1,1]blocks
	guide_addr    string
	guide_listen  net.Listener
	server_addr   string
	server_listen net.Listener
}

func newP2PBroadcastManager() *P2PBroadcastManager {
	hostname, _ := os.Hostname()
	return &P2PBroadcastManager{
		published: make(map[string][][]byte),
		cache:     NewCache(),
		host:      hostname,
	}
}

func (p *P2PBroadcastManager) Start(is_master bool) {
	if is_master {
		p.guides = make(map[string]map[string][]int)
		p.guide_addr, p.guide_listen = p.start_guide()
		_env.Register("BroadcastGuideAddr", p.guide_addr)
	} else {
		p.guide_addr = _env.Get("BroadcastGuideAddr", "").(string)
	}
	glog.Infof("broadcast started: tcp://%s", p.guide_addr)
}

func (p *P2PBroadcastManager) Shutdown() {
	item := &ActionItem{
		Action: "guide_stop",
	}

	if _, err := SendRPC(p.guide_addr, "RPC_Broadcast.Send", item); err != nil {
		glog.Fatalf("P2PBroadcastManager shutdown got error: %#v ", err)
	}
}

func SendRPC(addr string, action string, item *ActionItem) (*ReplyItem, error) {
	if client, err := rpc.Dial("tcp", addr); err != nil {
		return nil, err
	} else {
		defer client.Close()
		var reply ReplyItem
		if err = client.Call(action, item, &reply); err != nil {
			return nil, err
		}
		return &reply, nil
	}
}

func _BroadcastAcceptEx(lis net.Listener) {
	for {
		if conn, err := lis.Accept(); err != nil {
			//glog.Fatal(err)
		} else {
			go rpc.DefaultServer.ServeConn(conn)
		}
	}
}

type RPC_Broadcast struct {
	manager *P2PBroadcastManager
}

func NewRPC_Broadcast(p *P2PBroadcastManager) *RPC_Broadcast {
	return &RPC_Broadcast{
		manager: p,
	}
}

func (r *RPC_Broadcast) Send(item *ActionItem, reply *ReplyItem) error {
	action := strings.ToLower(item.Action)
	var result = "OK"
	switch action {
	case "guide_stop":
		glog.V(1).Info("Sending stop notification to all servers ...")
		for _, sources := range r.manager.guides {
			for addr := range sources {
				r.manager.stop_server(addr)
			}
		}
		r.manager.Stop() //guid stop
	case "sources":
		fmt.Printf("sources: %#v", item)
	case "report_bad":
		fmt.Printf("report_bad: %#v", item)
	default:
		fmt.Printf("Send: %#v", item)
		//TODO:log
		result = "ERROR"
	}
	reply.Result = result
	return nil
}

func (p *P2PBroadcastManager) Stop() {
	if p.guide_listen != nil {
		p.guide_listen.Close()
	}
}

func (p *P2PBroadcastManager) start_guide() (string, net.Listener) {
	r := NewRPC_Broadcast(p)
	rpc.Register(r)
	if tcpAddr, err := net.ResolveTCPAddr("tcp", "0.0.0.0:0"); err == nil {
		if lis, err := net.ListenTCP("tcp", tcpAddr); err == nil {
			addr := lis.Addr()
			if tcpAddr, ok := addr.(*net.TCPAddr); ok {
				port := tcpAddr.Port
				guide_addr := fmt.Sprintf("%s:%d", p.host, port)
				go func() {
					glog.Infof("guide start at tcp://%s", guide_addr)
					_BroadcastAcceptEx(lis)
				}()
				return guide_addr, lis
			}
		} else {
			glog.Fatal(err)
		}
	} else {
		glog.Fatal(err)
	}
	return "", nil
}

func (p *P2PBroadcastManager) start_server() (string, net.Listener) {
	r := NewRPC_Broadcast(p)
	rpc.Register(r)
	if tcpAddr, err := net.ResolveTCPAddr("tcp", "0.0.0.0:0"); err == nil {
		if lis, err := net.ListenTCP("tcp", tcpAddr); err == nil {
			addr := lis.Addr()
			if tcpAddr, ok := addr.(*net.TCPAddr); ok {
				port := tcpAddr.Port
				server_addr := fmt.Sprintf("%s:%d", p.host, port)
				go func() {
					glog.Infof("guide start at tcp://%s", server_addr)
					_BroadcastAcceptEx(lis)
				}()
				return server_addr, lis
			}
		} else {
			glog.Fatal(err)
		}
	} else {
		glog.Fatal(err)
	}
	return "", nil
}

func (p *P2PBroadcastManager) Register(uuid string, value interface{}) (uint32, error) {
	if _, ok := p.published[uuid]; ok {
		return 0, fmt.Errorf("broadcast %s has already registered", uuid)
	}

	if p.server_listen == nil {
		p.server_addr, p.server_listen = p.start_server()
	}

	if blocks, err := to_blocks(uuid, value); err == nil {
		p.published[uuid] = blocks
		m := make(map[string][]int)
		blocksArr := make([]int, len(blocks))
		for i := 0; i < len(blocks); i++ {
			blocksArr[i] = 1
		}
		m[p.server_addr] = blocksArr
		p.guides[uuid] = m
		p.cache.Put(uuid, value)
		return uint32(len(blocks)), nil
	} else {
		return 0, err
	}
}

func (p *P2PBroadcastManager) stop_server(addr string) {
	item := &ActionItem{
		Action: "server_stop",
	}
	SendRPC(addr, "RPC_Broadcast.Send", item)
}

func (p *P2PBroadcastManager) Fetch(uuid string, block_num uint32) (interface{}, error) {
	if p.server_listen == nil {
		p.server_addr, p.server_listen = p.start_server()
	}

	if value, ok := p.cache.Get(uuid); ok {
		return value, nil
	}
	var blocks [][]byte //p.fetch_blocks(uuid, block_num)
	if val, err := from_blocks(uuid, blocks); err != nil {
		return nil, err
	} else {
		return val, nil
	}
}

func (p *P2PBroadcastManager) Clear() {
}

type Broadcaster interface{}
type _Broadcast struct {
}

func newBroadcast(v interface{}) Broadcaster {
	return &_Broadcast{}
}
