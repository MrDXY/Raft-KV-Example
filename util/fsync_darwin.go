package util

import (
	"os"

	"golang.org/x/sys/unix"
)

func Fsync(f *os.File) error {
	_, err := unix.FcntlInt(f.Fd(), unix.F_FULLFSYNC, 0)
	return err
}

// Fdatasync on darwin platform invokes fcntl(F_FULLFSYNC) for actual persistence
// on physical drive media.
func Fdatasync(f *os.File) error {
	return Fsync(f)
}
