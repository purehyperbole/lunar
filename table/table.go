package table

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

const (
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
func (t *Table) Read(offset int64, size int64) ([]byte, error) {
	if int64(len(t.mapping)) < (offset + size) {
		return nil, ErrBoundsViolation
	}

	return t.mapping[offset:(offset + size)], nil
}

// Write : writes to table at a given offset
func (t *Table) Write(data []byte, offset int64) error {
	if (int64(len(t.mapping)) - offset) < int64(len(data)) {
		err := t.resize(int64(len(data)))
		if err != nil {
			return err
		}
	}

	copy(t.mapping[offset:], data)

	return nil
}

// Close : close table file descriptor and unmap
func (t *Table) Close() error {
	err := t.munmap()
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

	size := t.size()

	if size < int64(os.Getpagesize()) {
		size = t.sizeincrement()
		t.resize(size)
	}

	t.mapping, err = syscall.Mmap(int(t.fd.Fd()), 0, int(size), syscall.PROT_WRITE|syscall.PROT_READ, syscall.MAP_SHARED)

	return err
}

func (t *Table) munmap() error {
	return syscall.Munmap(t.mapping)
}

func (t *Table) resize(size int64) error {
	size = t.sizeincrement()

	err := t.fd.Truncate(t.size() + int64(size))
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

func (t *Table) size() int64 {
	stat, err := t.fd.Stat()
	if err != nil {
		return int64(0)
	}

	return stat.Size()
}

func (t *Table) sizeincrement() int64 {
	size := t.size() * 2

	if size < MinStep {
		size = MinStep
	}

	if size > MaxStep {
		size = MaxStep
	}

	if int(size)%os.Getpagesize() != 0 {
		fmt.Println(os.Getpagesize())
		fmt.Println("SIZE IS NOT A MULTIPLE OF PAGESIZE")
	}

	return size
}
