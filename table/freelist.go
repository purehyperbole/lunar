package table

import (
	"errors"
)

var (
	// ErrNoFreeSpace : returned when there is not enough space free that meets the criteria
	ErrNoFreeSpace = errors.New("there is not enough free space to allocate")
)

type alloc struct {
	offset int64
	size   int64
	next   *alloc
}

// FreeList : linked list implementation to track free space
type FreeList struct {
	root *alloc
}

// NewFreeList : creates a new freelist
func NewFreeList(maxsize int64) *FreeList {
	return &FreeList{
		root: &alloc{
			offset: 0,
			size:   maxsize,
			next:   nil,
		},
	}
}

// Reserve : reserves free space and returns an index that matches the given size criteria
func (f *FreeList) Reserve(size int64) (int64, error) {
	current := f.root

	for current != nil {
		if current.size >= size {
			off := current.offset
			current.offset = current.offset + size

			return off, nil
		}

		current = current.next
	}

	return -1, ErrNoFreeSpace
}

// Release : releases reserved space so it can be reused
func (f *FreeList) Release(size, offset int64) {
	current := f.root

	for current.offset < offset {
		current = current.next
	}

	prev := (*current)

	a := alloc{
		size:   size,
		offset: offset,
		next:   &prev,
	}

	(*current) = a
}
