/*
 * @Author: Malin Xie
 * @Description:
 * @Date: 2020-08-28 20:06:46
 */
package bigqueue

import (
	"os"
	"syscall"
	"unsafe"
)

// LockFileEx code derived from golang build filemutex_windows.go @ v1.5.1
var (
	modkernel32      = syscall.NewLazyDLL("kernel32.dll")
	procLockFileEx   = modkernel32.NewProc("LockFileEx")
	procUnlockFileEx = modkernel32.NewProc("UnlockFileEx")
)

const (
	// see https://msdn.microsoft.com/en-us/library/windows/desktop/aa365203(v=vs.85).aspx
	flagLockExclusive       = 2
	flagLockFailImmediately = 1

	// see https://msdn.microsoft.com/en-us/library/windows/desktop/ms681382(v=vs.85).aspx
	errLockViolation syscall.Errno = 0x21
)

// fdatasync flushes written data to a file descriptor.
func fdatasync(db *DB) error {
	return db.file.Sync()
}

// mmap memory maps a DB's data file.
// Based on: https://github.com/edsrzf/mmap-go
func mmap(db *DB, sz int) error {
	// Open a file mapping handle.
	sizelo := uint32(sz >> 32)
	sizehi := uint32(sz) & 0xffffffff
	h, errno := syscall.CreateFileMapping(syscall.Handle(db.file.Fd()), nil, syscall.PAGE_READWRITE, sizelo, sizehi, nil)
	if h == 0 {
		return os.NewSyscallError("CreateFileMapping", errno)
	}

	// Create the memory map.
	addr, errno := syscall.MapViewOfFile(h, syscall.FILE_MAP_WRITE, 0, 0, uintptr(sz))
	if addr == 0 {
		return os.NewSyscallError("MapViewOfFile", errno)
	}

	// Close mapping handle.
	if err := syscall.CloseHandle(syscall.Handle(h)); err != nil {
		return os.NewSyscallError("CloseHandle", err)
	}

	// Convert to a byte array.
	db.data = (*[maxMapSize]byte)(unsafe.Pointer(addr))
	return nil
}

// munmap unmaps a pointer from a file.
// Based on: https://github.com/edsrzf/mmap-go
func munmap(db *DB) error {
	if db.data == nil {
		return nil
	}

	addr := (uintptr)(unsafe.Pointer(&db.data[0]))
	if err := syscall.UnmapViewOfFile(addr); err != nil {
		return os.NewSyscallError("UnmapViewOfFile", err)
	}
	return nil
}
