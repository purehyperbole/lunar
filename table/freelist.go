package table

import (
	"errors"
	"sync"
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
	maxsize int64
	root    *alloc
	mu      sync.Mutex
}

// NewFreeList : creates a new freelist
func NewFreeList(maxsize int64) *FreeList {
	return &FreeList{
		maxsize: maxsize,
		root: &alloc{
			offset: 0,
			size:   maxsize,
			next:   nil,
		},
	}
}

// TODO : implement non-blocking linked list

// Reserve : reserves free space and returns an index that matches the given size criteria
func (f *FreeList) Reserve(size int64) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	current := f.root

	for current != nil {
		if current.size >= size {
			off := current.offset
			current.size = current.size - size
			current.offset = current.offset + size

			return off, nil
		}

		current = current.next
	}

	return -1, ErrNoFreeSpace
}

// Allocate : allocates a specified region as reserved
func (f *FreeList) Allocate(size, offset int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	current := f.root

	for current.offset+current.size <= offset {
		current = current.next
	}

	if current.offset == offset {
		current.offset = current.offset + size
		current.size = current.size - size
		return nil
	}

	next := (*current)

	a := alloc{
		size:   offset - next.offset,
		offset: next.offset,
		next:   &next,
	}

	next.offset = offset + size
	next.size = next.size - (offset + size)

	(*current) = a

	return nil
}

// Release : releases reserved space so it can be reused
func (f *FreeList) Release(size, offset int64) {
	f.mu.Lock()
	defer f.mu.Unlock()

	current := f.root

	// may need to improve this if free space that overlaps multiple regions
	for current.offset < offset && current != nil {
		current = current.next
	}

	if current == nil {
		return
	}

	if current.offset == size+offset {
		current.size = current.size + size + offset
		current.offset = current.offset - size
		return
	}

	prev := (*current)

	a := alloc{
		size:   size,
		offset: offset,
		next:   &prev,
	}

	(*current) = a
}

// Stats : returns the allocated space, and number of allocations
func (f *FreeList) Stats() (int64, int64) {
	var free int64
	var nodes int64

	current := f.root
	for current != nil {
		free = free + current.size
		nodes++
		current = current.next
	}

	allocspace := f.maxsize - free

	return allocspace, nodes
}

// Empty : returns true if no space has been allocated
func (f *FreeList) Empty() bool {
	return f.root.offset == 0 && f.root.next == nil
}

func overlaps(a *alloc, size, offset int64) bool {
	return true
}
