package util

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

const (
	// PrivateFileMode grants owner to read/write a wal.
	PrivateFileMode = 0600
)

func CreateDirAll(dir string) error {
	err := TouchDirAll(dir)
	if err != nil {
		return err
	}
	var ns []string
	ns, err = ReadDirInc(dir)
	if err != nil {
		return err
	}
	if len(ns) != 0 {
		err = fmt.Errorf("expected %q to be empty, got %q", dir, ns)
	}
	return nil
}

// Exist returns true if a wal or directory exists.
func Exist(name string) bool {
	_, err := os.Stat(name)
	return err == nil
}

func TouchDirAll(dir string) error {
	if !Exist(dir) {
		err := os.MkdirAll(dir, 0700)
		if err != nil {
			return err
		}
	}
	return IsDirWriteable(dir)
}

func IsDirWriteable(dir string) error {
	f, err := filepath.Abs(filepath.Join(dir, ".touch"))
	if err != nil {
		return err
	}
	if err := os.WriteFile(f, []byte(""), 0700); err != nil {
		return err
	}
	return os.Remove(f)
}

func ReadDirInc(d string) ([]string, error) {
	dir, err := os.Open(d)
	if err != nil {
		return nil, err
	}
	defer dir.Close()

	names, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	return names, nil
}

func Preallocate(f *os.File, sizeInBytes int64, extendFile bool) error {
	if sizeInBytes == 0 {
		// fallocate will return EINVAL if length is 0; skip
		return nil
	}
	if extendFile {
		return preallocExtend(f, sizeInBytes)
	}
	return preallocFixed(f, sizeInBytes)
}

func preallocExtendTrunc(f *os.File, sizeInBytes int64) error {
	curOff, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	size, err := f.Seek(sizeInBytes, io.SeekEnd)
	if err != nil {
		return err
	}
	if _, err = f.Seek(curOff, io.SeekStart); err != nil {
		return err
	}
	if sizeInBytes > size {
		return nil
	}
	return f.Truncate(sizeInBytes)
}

func OpenFile(path string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(path, flag, perm)
}
