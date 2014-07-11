package main

import (
	"fmt"
	"github.com/xiexiao/gopark/moosefs"
	"time"
)

var (
	host string = "172.16.0.13"
	port int32  = 9421
)

func main() {
	fmt.Println(time.Now())
	m := moosefs.NewMasterConn(host, port)
	defer m.Close()
	err := m.Connect()
	if err != nil {
		fmt.Printf("Connect error : %v\n", err)
		return
	}

	info, err2 := m.Lookup(1, "test.csv")
	fmt.Printf("%d, %#v %#v\n", info.Inode, info, err2)

	chunks, err3 := m.ReadChunk(info.Inode, 0)
	fmt.Printf("%#v  %#v  %#v \n", chunks, chunks.Addrs, err3)
	for i := 0; i < 100; i++ {
		info4, err4 := m.Lookup(1, "test.csv")
		chunks4, err5 := m.ReadChunk(info4.Inode, 0)
		fmt.Printf("%d, %#v, %#v, %#v \n", i, err4, err5, chunks4)
		time.Sleep(10 * time.Millisecond)
	}
	fmt.Println(time.Now())
}
