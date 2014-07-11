package moosefs

import (
	"testing"
)

var host = "172.16.0.13"
var port int32 = 9421

func Test_MasterConnect(t *testing.T) {
	m := NewMasterConn(host, port)
	defer m.Close()
	err := m.Connect()
	if err != nil {
		t.Errorf("Connect error : %v", err)
	}
	t.Logf("%v", m)
}
func Test_MasterStatfs(t *testing.T) {
	m := NewMasterConn(host, port)
	defer m.Close()
	err := m.Connect()
	if err != nil {
		t.Errorf("Connect error : %v", err)
	}
	statInfo, _ := m.Statfs()
	t.Logf("%v", statInfo)
}

func Test_MasterGetDir(t *testing.T) {
	m := NewMasterConn(host, port)
	defer m.Close()
	err := m.Connect()
	if err != nil {
		t.Errorf("Connect error : %v", err)
	}
	t.Logf("%v", m)

	names, err := m.GetDirPlus(1)
	if err == nil {
		t.Logf("#%v", names)
	} else {
		t.Errorf("getdir error : %v", err)
	}
}
func Test_MasterLookup(t *testing.T) {
	m := NewMasterConn(host, port)
	defer m.Close()
	err := m.Connect()
	if err != nil {
		t.Errorf("Connect error : %v", err)
	}
	info, err1 := m.Lookup(1, "test.csv")
	if err1 == nil {
		t.Logf("%v", info)
		if info.Length == 0 {
			t.Errorf("Lookup error : %s file size is 0.", info.Name)
		}
	} else {
		t.Fatalf("Lookup error : %v", err1)
	}

}

func Test_MasterReadChunk(t *testing.T) {
	m := NewMasterConn(host, port)
	defer m.Close()
	err := m.Connect()
	if err != nil {
		t.Errorf("Connect error : %v", err)
	}
	info, err2 := m.Lookup(1, "test.csv")

	if err2 == nil {
		if chunk, err := m.ReadChunk(info.Inode, 0); err != nil {
			t.Error(err)
		} else {
			t.Logf("%#v", chunk)
		}
	} else {
		t.Errorf("Lookup error : %v", err)
	}
}
