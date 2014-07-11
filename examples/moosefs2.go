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
	m := moosefs.NewMasterConn(host, port)
	fmt.Printf("%v\n", m)
	err := m.Connect()
	fmt.Println("Connected")
	if err == nil {
		if names, err := m.GetDir(0); err == nil {
			fmt.Printf("%#v\n", names)
		} else {
			fmt.Printf("%#v\n", err)
		}
	} else {
		fmt.Println(err)
	}
	time.Sleep(1 * time.Second)
}
