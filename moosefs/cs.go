package moosefs

import (
	"bytes"
	"fmt"
	"github.com/golang/glog"
	"os/exec"
	"time"
	//"io/ioutil"
	"bufio"
	"errors"
	"net"
	"os"
	"path"
	"strings"
)

var mfsdirs []string

func _scan() {
	cmd := `ps -eo cmd| grep mfschunkserver | grep -v grep | head -1 | cut -d ' ' -f1 | xargs dirname | sed 's#sbin##g'`
	mfs_prefix, err := exec.Command("/bin/sh", "-c", cmd).Output()
	if err == nil {
		mfs_cfg := fmt.Sprintf("%s/etc/mfshdd.cfg", mfs_prefix)
		mfs_cfg_list := []string{mfs_cfg, "/etc/mfs/mfshdd.cfg",
			"/etc/mfshdd.cfg", "/usr/local/etc/mfshdd.cfg"}
		for _, conf := range mfs_cfg_list {
			if _, ok := isFileDirExists(conf); !ok {
				continue
			}
			if file, err := os.Open(conf); err == nil {
				defer file.Close()
				scanner := bufio.NewScanner(file)
				for scanner.Scan() {
					fpath := strings.Trim(scanner.Text(), "#* \n")
					if _, ok := isFileDirExists(fpath); ok {
						mfsdirs = append(mfsdirs, fpath)
					}
				}
			}
		}
	}
}

func init() {
	_scan()
}

var CHUNKHDRSIZE uint64 = 1024 * 5

// offset =0
func read_chunk_from_local(done chan bool, chunkid uint64,
	version uint32, size uint64, offset uint64) (<-chan interface{}, error) {
	if offset+size > CHUNKSIZE {
		return nil, errors.New(fmt.Sprintf("size too large %d > %d", size, CHUNKSIZE-offset))
	}
	name := fmt.Sprintf("%02X/chunk_%016X_%08X.mfs", chunkid&0xFF, chunkid, version)
	for _, d := range mfsdirs {
		p := path.Join(d, name)
		if fi, ok := isFileDirExists(p); ok && !fi.IsDir() {
			if uint64(fi.Size()) < CHUNKHDRSIZE+offset+size {
				msg := fmt.Sprintf("%s is not completed: %d < %d", name, fi.Size(), CHUNKHDRSIZE+offset+size)
				glog.Error(msg)
				return nil, errors.New(msg)
			}
			if f, err := os.Open(p); err != nil {
				return nil, err
			} else {
				ch := make(chan interface{})
				go func(f *os.File) {
					defer close(ch)
					defer f.Close()
					f.Seek(int64(CHUNKHDRSIZE+offset), 0)
					for size > 0 {
						to_read := MinUint64(size, 640*1024)
						var data = make([]byte, to_read)
						if n, err := f.Read(data); err == nil {
							size = size - uint64(n)
							//LocalReadBytes.add(len(data))
							select {
							case ch <- data[:n]:
							case <-done:
								return
							}
						} else {
							ChanError(done, ch, err)
							return
						}
					}
				}(f)
				return ch, nil
			}
		}
	}
	glog.Warningf("%s was not found", name)
	return nil, nil
}

//offset =0
// return <-chan []byte , error  ,chan may be error
func read_chunk(done chan bool, host string, port uint32,
	chunkid uint64, version uint32, size uint64, offset uint64) (<-chan interface{}, error) {
	if offset+size > CHUNKSIZE {
		return nil, fmt.Errorf("size too large %d > %d", size, CHUNKSIZE-offset)
	}

	addr := fmt.Sprintf("%s:%d", host, port)
	tcpAddr, err := net.ResolveTCPAddr("tcp4", addr)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		return nil, err
	}
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	connClose := func() {
		conn.Close()
	}
	msg := Pack(CUTOCS_READ, Pack_Q(chunkid), version, offset, size)
	n, err := conn.Write(msg)
	if err == nil {
		for n < len(msg) {
			sent, err := conn.Write(msg[n:])
			if err != nil {
				connClose()
				return nil, err
			}
			n += sent
		}
	} else {
		connClose()
		return nil, err
	}

	recv := func(size uint32) ([]byte, error) {
		buf := new(bytes.Buffer)
		tmp := make([]byte, size)
		if n, err := conn.Read(tmp); err != nil {
			return nil, err
		} else {
			buf.Write(tmp[:n])
			for uint32(n) < size {
				recvSize, err := conn.Read(tmp)
				if err != nil {
					break
				}
				buf.Write(tmp[:recvSize])
				n += recvSize
			}
			return buf.Bytes(), nil
		}
	}
	ch := make(chan interface{}) //base []byte
	go func() {
		defer conn.Close()
		defer close(ch)
		for size > 0 {
			if buff, err := recv(8); err != nil {
				ChanError(done, ch, err)
				return
			} else {
				cmd, l := Unpack_II(buff)
				if cmd == CSTOCU_READ_STATUS {
					if l != 9 {
						ChanError(done, ch, errors.New("readblock: READ_STATUS incorrect message size"))
						return
					}

					if buf, err := recv(l); err != nil {
						ChanError(done, ch, err)
						return
					} else {
						cid, _ := Unpack_QB(buf)
						if cid != chunkid {
							ChanError(done, ch, errors.New("readblock; READ_STATUS incorrect chunkid"))
							return
						}
					}
				} else if cmd == CSTOCU_READ_DATA {
					if l < 20 {
						ChanError(done, ch, errors.New("readblock; READ_DATA incorrect message size"))
						return
					}
					if buf, err := recv(20); err != nil {
						ChanError(done, ch, err)
						return
					} else {
						cid, bid, boff, bsize, _ := Unpack_QHHII(buf)
						if cid != chunkid {
							ChanError(done, ch, errors.New("readblock; READ_STATUS incorrect chunkid"))
							return
						}
						if l != 20+bsize {
							ChanError(done, ch, errors.New("readblock; READ_DATA incorrect message size "))
							return
						}
						if bsize == 0 {
							ChanError(done, ch, errors.New("readblock; empty block"))
							return
						}
						if uint64(bid) != offset>>16 {
							ChanError(done, ch, errors.New("readblock; READ_DATA incorrect block number"))
							return
						}
						if uint64(boff) != offset&0xFFFF {
							ChanError(done, ch, errors.New("readblock; READ_DATA incorrect block offset"))
							return
						}
						breq := 65536 - uint64(boff)
						if size < breq {
							breq = size
						}
						if uint64(bsize) != breq {
							ChanError(done, ch, errors.New("readblock; READ_DATA incorrect block size"))
							return
						}
						for breq > 0 {
							tmp := make([]byte, breq)
							if n, err := conn.Read(tmp); err != nil {
								ChanError(done, ch, err)
								return
							} else {
								if n < 1 {
									ChanError(done, ch, fmt.Errorf("unexpected ending: need %d", breq))
									return
								}
								//RemoteReadBytes.add(n)
								breq -= uint64(n)
								select {
								case ch <- tmp[:n]:
								case <-done:
									return
								}
							}
						}
						offset += uint64(bsize)
						size -= uint64(bsize)
					}
				} else {
					ChanError(done, ch, fmt.Errorf("readblock; unknown message: %s", cmd))
					return
				}
			}
		}
	}()
	return ch, nil
}
