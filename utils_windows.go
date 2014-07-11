package gopark

import (
	"syscall"
	"unsafe"
)

func getPathFreeTotal(fpath string) (uint64, uint64) {
	h := syscall.MustLoadDLL("kernel32.dll")
	c := h.MustFindProc("GetDiskFreeSpaceExW")
	lpFreeBytesAvailable := uint64(0)
	lpTotalNumberOfBytes := uint64(0)
	lpTotalNumberOfFreeBytes := uint64(0)
	c.Call(uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr(fpath))),
		uintptr(unsafe.Pointer(&lpFreeBytesAvailable)),
		uintptr(unsafe.Pointer(&lpTotalNumberOfBytes)),
		uintptr(unsafe.Pointer(&lpTotalNumberOfFreeBytes)))
	return lpTotalNumberOfFreeBytes, lpTotalNumberOfBytes
}
