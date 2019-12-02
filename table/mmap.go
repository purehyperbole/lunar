package table

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

var (
	ErrMappingClosed = errors.New("mapping closed")
)

type mmap struct {
	fd      *os.File // file descriptor
	size    int64    // file Size
	active  int32    // active read or write operations
	closed  int32    // mapping is closed. probably should just use a rwmutex with write lock protecting the resize, but that would be no fun
	mapping []byte   // mmap mapping
}

func newmmap(fd *os.File) (*mmap, error) {
	stat, err := fd.Stat()
	if err != nil {
		return nil, err
	}

	m := mmap{
		fd:      fd,
		size:    stat.Size(),
		mapping: make([]byte, 0),
	}

	return &m, m.mmap()
}

func (m *mmap) mmap() error {
	mapping, err := syscall.Mmap(
		int(m.fd.Fd()),
		0,
		int(m.size),
		syscall.PROT_WRITE|syscall.PROT_READ, syscall.MAP_SHARED,
	)

	if err != nil {
		return err
	}

	m.mapping = mapping

	return nil
}

func (m *mmap) munmap() error {
	fmt.Println("munmap", m.size)
	return syscall.Munmap(m.mapping)
}

func (m *mmap) mremap(newSize int64) error {
	sh := (*reflect.SliceHeader)(unsafe.Pointer(&m.mapping))

	r1, _, err := syscall.Syscall6(syscall.SYS_MREMAP, sh.Data, uintptr(sh.Len), uintptr(newSize), uintptr(1), 0, 0)
	if err != 0 {
		return syscall.Errno(err)
	}

	nsh := &reflect.SliceHeader{
		Data: r1,
		Len:  int(newSize),
		Cap:  int(newSize),
	}

	m.mapping = *(*[]byte)(unsafe.Pointer(nsh))

	return nil
}

func (m *mmap) read(size, offset int64) ([]byte, error) {
	if atomic.AddInt32(&m.active, 1) < 1 {
		return nil, ErrMappingClosed
	}

	defer atomic.AddInt32(&m.active, -1)

	if m.size < (offset + size) {
		return nil, ErrBoundsViolation
	}

	return m.mapping[offset:(offset + size)], nil
}

func (m *mmap) write(data []byte, offset int64) error {
	atomic.AddInt32(&m.active, 1)
	defer atomic.AddInt32(&m.active, -1)

	if atomic.LoadInt32(&m.closed) == 1 {
		fmt.Println("mapping closed")
		return ErrMappingClosed
	}

	if len(data) > MaxStep {
		return ErrDataSizeTooLarge
	}

	if m.size < (offset + int64(len(data))) {
		return ErrBoundsViolation
	}

	copy(m.mapping[offset:], data)

	return nil
}

func (m *mmap) close() error {
	atomic.StoreInt32(&m.closed, 1)
	for atomic.LoadInt32(&m.active) > 0 {
		time.Sleep(time.Millisecond)
	}
	return m.munmap()
}
