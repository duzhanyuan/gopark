package gopark

import (
	"encoding/gob"
	"testing"
)

type ABC struct {
	A string
	B int
	C float64
}

func TestDiskCache(t *testing.T) {
	gob.Register(new(ABC))
	c := newDiskCache(nil, "C:\\tmp\\cache123")
	defer c.Clear()
	c.Put("abc", ABC{
		A: "google",
		C: 1024.89,
	})
	c.Put("abcbbb", "abccc")
	if abc, ok := c.Get("abc"); ok {
		for _, x := range abc {
			if item, ok := x.(*ABC); ok {
				t.Logf("===>%#v\n", item)
			}
		}
		t.Logf("get: %#v\n", abc)
	}
	t.Log("ok")
}

func TestStringSet(t *testing.T) {
	set := NewStringSet()
	set.Add("abc")
	set.Add("ddd")
	set.Add("abc")
	for idx, x := range set.GetList() {
		t.Logf("set item: %d, val: %s", idx, x)
	}
	t.Logf("set len: %d", set.GetLength())
	set.Clear()
	t.Logf("set len: %d", set.GetLength())
}

func TestParseHostname(t *testing.T) {
	uri := "http://abc.com"
	if parseHostname(uri) != "abc.com" {
		t.Error("parseHostname")
	}
	uri = "http://abc.com/"
	if parseHostname(uri) != "" {
		t.Error("parseHostname")
	}
	uri = "http://abc.com/xiexie"
	if parseHostname(uri) != "xiexie" {
		t.Error("parseHostname")
	}
	uri = "http://abc.com/xie/xie"
	if parseHostname(uri) != "xie" {
		t.Error("parseHostname")
	}
}
