package table

import (
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"unsafe"
)

const (
	// PageSize page size
	PageSize = 1 << 12 // 4kb
	// MinStep the smallest increment that the table can grow
	MinStep = 1 << 16 // 64 kb
	// MaxStep the largest increment that the table can grow
	MaxStep = 1 << 30 // 1 GB
	// MaxTableSize maximum size of table
	MaxTableSize = 0x7FFFFFFFFFFFFFFF
)

var (
	// ErrBoundsViolation the specified segment of memory does not exist
	ErrBoundsViolation = errors.New("specified offset and size exceeds size of mapping")
	// ErrDataSizeTooLarge the provided value data exceeds the maximum size limit
	ErrDataSizeTooLarge = errors.New("data exceeds maximum limit")
)

// Table mmaped file
type Table struct {
	fd       *os.File
	position int64
	mapping  unsafe.Pointer
	mu       sync.Mutex
}

// New loads a new table
func New(path string) (*Table, error) {
	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0766)
	if err != nil {
		return nil, err
	}

	stat, err := fd.Stat()
	if err != nil {
		return nil, err
	}

	if stat.Size() < 1 {
		err = fd.Truncate(MinStep)
		if err != nil {
			return nil, err
		}
	}

	mapping, err := newmmap(fd)
	if err != nil {
		return nil, err
	}

	t := Table{
		fd:      fd,
		mapping: unsafe.Pointer(mapping),
	}

	return &t, nil
}

// Read reads from table at a given offset
func (t *Table) Read(size, offset int64) ([]byte, error) {
	mapping := (*mmap)(atomic.LoadPointer(&t.mapping))
	return mapping.read(size, offset)
}

// Write writes to table at a given offset
func (t *Table) Write(data []byte) (int64, error) {
	ds := int64(len(data))

	currentSize := t.size()
	offset := atomic.AddInt64(&t.position, ds) - ds

	if currentSize < offset+ds {
		err := t.resize(ds, offset)
		if err != nil {
			return 0, err
		}
	}

	mapping := (*mmap)(atomic.LoadPointer(&t.mapping))

	err := mapping.write(data, offset)
	if err != nil {
		return 0, err
	}

	return offset, nil
}

// Close close table file descriptor and unmap
func (t *Table) Close() error {
	mapping := (*mmap)(atomic.LoadPointer(&t.mapping))

	err := mapping.close()
	if err != nil {
		return err
	}

	err = t.sync()
	if err != nil {
		return err
	}

	return t.fd.Close()
}

func (t *Table) sync() error {
	return t.fd.Sync()
}

func (t *Table) size() int64 {
	return (*mmap)(atomic.LoadPointer(&t.mapping)).size
}

func (t *Table) resize(size, offset int64) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.size() > size+offset {
		return nil
	}

	newSize := t.growadvise(size)

	err := t.fd.Truncate(newSize)
	if err != nil {
		return err
	}

	oldMapping := (*mmap)(t.mapping)

	newMapping, err := newmmap(t.fd)
	if err != nil {
		return err
	}

	atomic.StorePointer(&t.mapping, unsafe.Pointer(newMapping))

	if oldMapping != nil {
		return oldMapping.close()
	}

	return nil
}

func (t *Table) growadvise(size int64) int64 {
	tsz := t.size()

	if size < tsz {
		size = tsz * 2
	}

	if size < MinStep {
		return tsz + MinStep
	}

	if size > MaxStep {
		return tsz + MaxStep
	}

	return size + size%PageSize
}
