// +build linux darwin

package gopark

import (
	"github.com/golang/glog"
	"syscall"
)

func getPathFreeTotal(fpath string) (uint64, uint64) {
	st := &syscall.Statfs_t{}
	if err := syscall.Statfs(fpath, st); err != nil {
		glog.Fatal(err)
	} else {
		free := st.Bfree * uint64(st.Bsize)
		total := st.Blocks * uint64(st.Bsize)
		return free, total
	}
	return 0, 0
}
