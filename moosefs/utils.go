package moosefs

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"os"
)

func MaxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func MinUint64(a, b uint64) uint64 {
	if a > b {
		return b
	}
	return a
}

func MaxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func MinInt64(a, b int64) int64 {
	if a > b {
		return b
	}
	return a
}

func packValues(buf *bytes.Buffer, val interface{}) {
	if i, ok := val.([]byte); ok {
		buf.Write(i)
	} else if vals, ok := val.([]interface{}); ok {
		for _, v := range vals {
			packValues(buf, v)
		}
	} else if i, ok := val.(uint8); ok {
		binary.Write(buf, binary.BigEndian, i)
	} else if i, ok := val.(int); ok {
		binary.Write(buf, binary.BigEndian, uint32(i))
	} else if i, ok := val.(int32); ok {
		binary.Write(buf, binary.BigEndian, uint32(i))
	} else if i, ok := val.(uint32); ok {
		if i >= math.MaxUint32 {
			binary.Write(buf, binary.BigEndian, uint32(0))
		} else {
			binary.Write(buf, binary.BigEndian, i)
		}
	} else if i, ok := val.(int64); ok {
		if i > math.MaxUint32 {
			binary.Write(buf, binary.BigEndian, uint32(0))
		} else {
			binary.Write(buf, binary.BigEndian, uint32(i))
		}
	} else if i, ok := val.(uint64); ok {
		if i > math.MaxUint32 {
			binary.Write(buf, binary.BigEndian, uint32(0))
		} else {
			binary.Write(buf, binary.BigEndian, uint32(i))
		}
	} else if i, ok := val.(string); ok {
		buf.WriteString(i)
	}
}

func Pack_Q(i uint64) []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, i)
	return buf.Bytes()
}

func Pack(cmd uint32, args ...interface{}) []byte {
	buf := new(bytes.Buffer)
	for _, n := range args {
		packValues(buf, n)
	}
	length := buf.Len()
	buf2 := new(bytes.Buffer)
	binary.Write(buf2, binary.BigEndian, cmd)
	binary.Write(buf2, binary.BigEndian, uint32(length))
	buf2.Write(buf.Bytes())
	return buf2.Bytes()
}
func Pack_I(args ...interface{}) []byte {
	buf := new(bytes.Buffer)
	for _, n := range args {
		//TOOD:convert to uint32, uint64
		if i, ok := n.(int); ok {
			binary.Write(buf, binary.BigEndian, uint32(i))
		} else if i, ok := n.(int32); ok {
			binary.Write(buf, binary.BigEndian, uint32(i))
		} else if i, ok := n.(uint8); ok {
			binary.Write(buf, binary.BigEndian, uint32(i))
		} else if i, ok := n.(uint16); ok {
			binary.Write(buf, binary.BigEndian, uint32(i))
		} else if i, ok := n.(uint32); ok {
			binary.Write(buf, binary.BigEndian, i)
		} else if i, ok := n.(int64); ok {
			binary.Write(buf, binary.BigEndian, uint64(i))
		} else if i, ok := n.(uint64); ok {
			binary.Write(buf, binary.BigEndian, i)
		} else if i, ok := n.(string); ok {
			buf.WriteString(i)
		}
	}
	return buf.Bytes()
}

func Unpack_QQQQI(buf []byte) (uint64, uint64, uint64, uint64, uint32) {
	var i1, i2, i3, i4 uint64
	var i5 uint32
	binary.Read(bytes.NewBuffer(buf[0:8]), binary.BigEndian, &i1)
	binary.Read(bytes.NewBuffer(buf[8:16]), binary.BigEndian, &i2)
	binary.Read(bytes.NewBuffer(buf[16:24]), binary.BigEndian, &i3)
	binary.Read(bytes.NewBuffer(buf[24:32]), binary.BigEndian, &i4)
	binary.Read(bytes.NewBuffer(buf[32:36]), binary.BigEndian, &i4)
	return i1, i2, i3, i4, i5
}

//QHHII
func Unpack_QHHII(buf []byte) (uint64, uint16, uint16, uint32, uint32) {
	return binary.BigEndian.Uint64(buf[0:8]),
		binary.BigEndian.Uint16(buf[8:10]),
		binary.BigEndian.Uint16(buf[10:12]),
		binary.BigEndian.Uint32(buf[12:16]),
		binary.BigEndian.Uint32(buf[16:20])
}

func Unpack_QQI(buf []byte) (uint64, uint64, uint32) {
	var i1, i2 uint64
	var i3 uint32
	binary.Read(bytes.NewBuffer(buf[0:8]), binary.BigEndian, &i1)
	binary.Read(bytes.NewBuffer(buf[8:16]), binary.BigEndian, &i2)
	binary.Read(bytes.NewBuffer(buf[16:20]), binary.BigEndian, &i3)
	return i1, i2, i3
}

// III
func Unpack_III(buf []byte) (uint32, uint32, uint32) {
	var i1, i2, i3 uint32
	binary.Read(bytes.NewBuffer(buf[0:4]), binary.BigEndian, &i1)
	binary.Read(bytes.NewBuffer(buf[4:8]), binary.BigEndian, &i2)
	binary.Read(bytes.NewBuffer(buf[8:12]), binary.BigEndian, &i3)
	return i1, i2, i3
}

// II
func Unpack_II(buf []byte) (uint32, uint32) {
	var cmd, i uint32
	binary.Read(bytes.NewBuffer(buf[0:4]), binary.BigEndian, &cmd)
	binary.Read(bytes.NewBuffer(buf[4:8]), binary.BigEndian, &i)
	return cmd, i
}

// I
func Unpack_I(buf []byte) uint32 {
	var i uint32
	p := bytes.NewBuffer(buf)
	binary.Read(p, binary.BigEndian, &i)
	return i
}

//IB
func Unpack_IB(buf []byte) (uint32, uint8) {
	var i1 uint32
	var i2 uint8
	binary.Read(bytes.NewBuffer(buf[0:4]), binary.BigEndian, &i1)
	binary.Read(bytes.NewBuffer(buf[4:5]), binary.BigEndian, &i2)
	return i1, i2
}

//QB
func Unpack_QB(buf []byte) (uint64, uint8) {
	return binary.BigEndian.Uint64(buf[0:8]), buf[8]
}

//B
func Unpack_B(buf []byte) uint8 {
	var i1 uint8
	p := bytes.NewBuffer(buf)
	binary.Read(p, binary.BigEndian, &i1)
	return i1
}

//H
func Unpack_H(buf []byte) uint16 {
	var i1 uint16
	p := bytes.NewBuffer(buf)
	binary.Read(p, binary.BigEndian, &i1)
	return i1
}

//Q
func Unpack_Q(buf []byte) uint64 {
	var i1 uint64
	p := bytes.NewBuffer(buf)
	binary.Read(p, binary.BigEndian, &i1)
	return i1
}

type FileInfo struct {
	Type   uint8
	Mode   uint16
	Uid    uint32
	Gid    uint32
	Atime  uint32
	Mtime  uint32
	Ctime  uint32
	Nlink  uint32
	Length uint64
	Inode  uint32
	Name   string
	Blocks uint64
}

func (f FileInfo) String() string {
	return fmt.Sprintf("FileInfo(%s, Inode=%d, Type=%c, Length=%d)",
		f.Name, f.Inode, f.Type, f.Length)
}

func (f FileInfo) IsSymlink() bool {
	return f.Type == TYPE_SYMLINK
}

func attrToFileInfo(inode uint32, buf []byte, name string) *FileInfo {
	//"!BHIIIIIIQ"
	f := &FileInfo{
		Inode: inode,
		Name:  name,
	}

	//fmt.Println(buf)
	//p := bytes.NewReader(buf)
	//binary.Read(p, binary.BigEndian, f)
	//fmt.Printf("%#v, ===== \n", f)
	//binary.Read(bytes.NewBuffer(buf[0:1]), binary.BigEndian, &f.Type)

	f.Type = buf[0]                                     //B
	var mode uint16 = binary.BigEndian.Uint16(buf[1:3]) //H
	var nMode uint16
	if f.Type == TYPE_DIRECTORY {
		nMode = mode | S_IFDIR
	} else if f.Type == TYPE_SYMLINK {
		nMode = mode | S_IFLNK
	} else if f.Type == TYPE_FILE {
		nMode = mode | S_IFREG
	}
	f.Mode = nMode
	f.Uid = binary.BigEndian.Uint32(buf[3:7])  //I
	f.Gid = binary.BigEndian.Uint32(buf[7:11]) //I

	f.Atime = binary.BigEndian.Uint32(buf[11:15])  //I
	f.Mtime = binary.BigEndian.Uint32(buf[15:19])  //I
	f.Ctime = binary.BigEndian.Uint32(buf[19:23])  //I
	f.Nlink = binary.BigEndian.Uint32(buf[23:27])  //I
	f.Length = binary.BigEndian.Uint64(buf[27:35]) //Q

	f.Blocks = (f.Length + 511) / 512
	return f
}

func isFileDirExists(path string) (os.FileInfo, bool) {
	fi, err := os.Stat(path)
	return fi, os.IsExist(err)
}

func ChanError(done chan bool, ch chan interface{}, err error) {
	select {
	case ch <- err:
		return
	case <-done:
		return
	}
}
