package util

import (
	"errors"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

func preallocExtend(f *os.File, sizeInBytes int64) error {
	if err := preallocFixed(f, sizeInBytes); err != nil {
		return err
	}
	return preallocExtendTrunc(f, sizeInBytes)
}

func preallocFixed(f *os.File, sizeInBytes int64) error {
	// allocate all requested space or no space at all
	// TODO: allocate contiguous space on disk with F_ALLOCATECONTIG flag
	fstore := &unix.Fstore_t{
		Flags:   unix.F_ALLOCATEALL,
		Posmode: unix.F_PEOFPOSMODE,
		Length:  sizeInBytes,
	}
	err := unix.FcntlFstore(f.Fd(), unix.F_PREALLOCATE, fstore)
	if err == nil || errors.Is(err, unix.ENOTSUP) {
		return nil
	}

	// wrong argument to fallocate syscall
	if errors.Is(err, unix.EINVAL) {
		// filesystem "st_blocks" are allocated in the units of
		// "Allocation Block Size" (run "diskutil info /" command)
		var stat syscall.Stat_t
		syscall.Fstat(int(f.Fd()), &stat)

		// syscall.Statfs_t.Bsize is "optimal transfer block size"
		// and contains matching 4096 value when latest OS X kernel
		// supports 4,096 KB filesystem block size
		var statfs syscall.Statfs_t
		syscall.Fstatfs(int(f.Fd()), &statfs)
		blockSize := int64(statfs.Bsize)

		if stat.Blocks*blockSize >= sizeInBytes {
			// enough blocks are already allocated
			return nil
		}
	}
	return err
}
