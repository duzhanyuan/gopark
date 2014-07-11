package gopark

import (
	"encoding/gob"
	"github.com/nu7hatch/gouuid"
	"testing"
)

type ABC1 struct {
	A string
	B float64
	C bool
}

func TestTo_Blocks(t *testing.T) {
	uuid := "abc"
	data := "bbc"
	if blocks, err := to_blocks(uuid, data); err != nil {
		t.Fatal(err)
	} else {
		//t.Logf("%#v", blocks)
		if obj, err2 := from_blocks(uuid, blocks); err2 != nil {
			t.Fatal(err2)
		} else {
			if obj == nil {
				t.Fatal("obj is nil")
			}
			//t.Logf("%#v", obj)
		}
	}
}

func TestTo_BlocksStruct(t *testing.T) {
	_uuid, _ := uuid.NewV4()
	uuid := _uuid.String()
	data := &ABC1{
		A: "ok",
		B: 123,
		C: true,
	}
	gob.Register(new(ABC1))
	if blocks, err := to_blocks(uuid, data); err != nil {
		t.Fatal(err)
	} else {
		t.Logf("%#v", blocks)
		if obj, err2 := from_blocks(uuid, blocks); err2 != nil {
			t.Fatal(err2)
		} else {
			t.Logf("%#v", obj)
			if abc1, ok := obj.(*ABC1); ok {
				if data.A != abc1.A {
					t.Fatal("blocks errors....")
				}
			} else {
				t.Logf("%#v, %#v\n", abc1, ok)
				t.Fatal("blocks errors....")
			}
		}
	}
}
