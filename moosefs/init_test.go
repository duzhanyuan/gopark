package moosefs_test

import (
	"fmt"
	"github.com/xiexiao/gopark/moosefs"
	"testing"
)

func Test_MFSOpen(t *testing.T) {
	fs, err := moosefs.MFSOpen("/test.csv", "172.16.0.13")
	if err != nil {
		t.Error("MFSOpen error", err)
	}
	t.Logf("%#v\n", fs)
	chunk, err := fs.Get_Chunk(0)
	t.Logf("%#v, %#v\n", chunk, err)
}

func Test_GetMFS(t *testing.T) {
	fs := moosefs.GetMFS("172.16.0.13")
	if f, err := fs.Lookup("/files/node/mesos-0.18.0-0.x86_64.rpm", true); err == nil {
		t.Logf("%#v, %#v\n", f, err)
		if f != nil {
			t.Logf("Inode:%d Blocks:%d\n", f.Inode, f.Blocks)
		}
	} else {
		t.Error("GetMFS error", err)
	}
}

func Test_Walk(t *testing.T) {
	fs := moosefs.GetMFS("172.16.0.13")
	defer fs.Close()
	done := make(chan bool)
	defer close(done)
	f := fs.Walk(done, "/", true)

	for val := range f {
		if item, ok := val.(moosefs.WalkItem); ok {
			t.Log(fmt.Sprintf("(%s %s %s \n\n", item.Root, item.Dirs, item.Files))
			break
			//fmt.Printf("(%s %s %s \n\n", item.Root, item.Dirs, item.Files)
		} else {
			t.Fatalf("%#v \n", item)
		}
	}
}
