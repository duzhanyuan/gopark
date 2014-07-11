package moosefs

import (
	"testing"
)

func Test_Pack1(t *testing.T) {
	buf := Pack(1, "ABC")
	t.Log(string(buf))
	if buf == nil {
		t.Error("pack error")
	}
}
