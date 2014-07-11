package moosefs

import (
	"testing"
)

func Test_read_chunk(t *testing.T) {
	host := "172.16.0.13"
	var port uint32 = 9422
	var chunkid uint64 = 503
	var version uint32 = 1
	var size uint64 = 7
	done := make(chan bool)
	defer close(done)
	if ch, err := read_chunk(done, host, port, chunkid, version, size, 0); err != nil {
		t.Error(err)
	} else {
		var total uint64
		for c := range ch {
			if data, ok := c.([]byte); ok {
				total += uint64(len(data))
			} else {
				t.Errorf("%#v, %s", c, c)
			}
		}
		t.Logf("%d", total)
	}
}
