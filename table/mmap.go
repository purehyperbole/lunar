package table

import (
	"os"
	"syscall"
)

type mmap struct {
	fd      *os.File // file descriptor
	size    int64    // file Size
	active  int64    // active read or write operations
	mapping []byte   // mmap mapping
}

func newmmap(fd *os.File) (*mmap, error) {
	stat, err := fd.Stat()
	if err != nil {
		return nil, err
	}

	m := mmap{
		fd:   fd,
		size: stat.Size(),
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
	return syscall.Munmap(m.mapping)
}
