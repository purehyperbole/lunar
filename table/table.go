package table

import (
	"errors"
	"os"
	"reflect"
	"syscall"
	"unsafe"
)

const (
	// PageSize : page size
	PageSize = 1 << 12 // 4kb
	// MinStep : the smallest increment that the table can grow
	MinStep = 1 << 16 // 64 kb
	// MaxStep : the largest increment that the table can grow
	MaxStep = 1 << 30 // 1 GB
	// MaxTableSize : maximum size of table
	MaxTableSize = 0x7FFFFFFFFFFFFFFF
)

var (
	// ErrBoundsViolation : the specified segment of memory does not exist
	ErrBoundsViolation = errors.New("specified offset and size exceeds size of mapping")
	// ErrDataSizeTooLarge : the provided value data exceeds the maximum size limit
	ErrDataSizeTooLarge = errors.New("data exceeds maximum limit")
)

// Table : mmaped file
type Table struct {
	Free    *FreeList
	fd      *os.File
	mapping []byte
}

// New : loads a new table
func New(path string) (*Table, error) {
	t := Table{
		Free:    NewFreeList(MaxTableSize),
		mapping: make([]byte, 0),
	}

	err := t.open(path)
	if err != nil {
		return nil, err
	}

	return &t, t.mmap()
}

// Read : reads from table at a given offset
func (t *Table) Read(size, offset int64) ([]byte, error) {
	if int64(len(t.mapping)) < (offset + size) {
		return nil, ErrBoundsViolation
	}

	return t.mapping[offset:(offset + size)], nil
}

// Write : writes to table at a given offset
func (t *Table) Write(data []byte, offset int64) error {
	if len(data) > MaxStep {
		return ErrDataSizeTooLarge
	}

	if (int64(len(t.mapping)) - offset) < int64(len(data)) {
		err := t.resize(int64(len(data)))
		if err != nil {
			return err
		}
	}

	copy(t.mapping[offset:], data)

	return nil
}

// Sync : syncs the tables mapping to disk
func (t *Table) Sync() error {
	sh := (*reflect.SliceHeader)(unsafe.Pointer(&t.mapping))

	_, _, err := syscall.Syscall(syscall.SYS_MSYNC, sh.Data, uintptr(sh.Len), syscall.MS_SYNC)
	if err != 0 {
		return syscall.Errno(err)
	}

	return nil
}

// Close : close table file descriptor and unmap
func (t *Table) Close() error {
	err := t.Sync()
	if err != nil {
		return err
	}

	err = t.munmap()
	if err != nil {
		return err
	}

	return t.fd.Close()
}

func (t *Table) open(path string) error {
	var err error

	t.fd, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0766)

	return err
}

func (t *Table) mmap() error {
	var err error

	size := t.Size()

	if size < PageSize {
		t.resize(size)
		size = t.Size()
	}

	t.mapping, err = syscall.Mmap(int(t.fd.Fd()), 0, int(size), syscall.PROT_WRITE|syscall.PROT_READ, syscall.MAP_SHARED)

	return err
}

func (t *Table) munmap() error {
	return syscall.Munmap(t.mapping)
}

func (t *Table) mremap() error {
	sh := (*reflect.SliceHeader)(unsafe.Pointer(&t.mapping))

	// doesn't work, returns invalid argument. incorrect flags/args?
	_, _, err := syscall.Syscall(syscall.SYS_MREMAP, sh.Data, uintptr(sh.Len), 0)
	if err != 0 {
		return syscall.Errno(err)
	}

	return nil
}

func (t *Table) resize(size int64) error {
	err := t.fd.Truncate(t.growadvise(size))
	if err != nil {
		return err
	}

	err = t.fd.Sync()
	if err != nil {
		return err
	}

	err = t.munmap()
	if err != nil {
		return err
	}

	return t.mmap()
}

// Size : Returns size in bytes
func (t *Table) Size() int64 {
	stat, err := t.fd.Stat()
	if err != nil {
		return int64(0)
	}

	return stat.Size()
}

func (t *Table) growadvise(size int64) int64 {
	if size < t.Size() {
		size = t.Size() * 2
	}

	if size < MinStep {
		return t.Size() + MinStep
	}

	if size > MaxStep {
		return t.Size() + MaxStep
	}

	return size + size%PageSize
}
