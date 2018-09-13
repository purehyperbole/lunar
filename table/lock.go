package table

import (
	"sync"
)

// PageLock : handles locking for index nodes
type PageLock struct {
	mu    sync.Mutex
	locks map[int64]*sync.RWMutex
}

// NewPageLock : creates a new page lock for coordinating reads and writes to db index
func NewPageLock() *PageLock {
	return &PageLock{
		locks: make(map[int64]*sync.RWMutex),
	}
}

// Lock : locks a node at a specific offset
func (wl *PageLock) Lock(offset int64, readonly bool) {
	wl.mu.Lock()

	if wl.locks[offset] == nil {
		wl.locks[offset] = &sync.RWMutex{}
	}

	l := wl.locks[offset]

	wl.mu.Unlock()

	if readonly {
		l.RLock()
	} else {
		l.Lock()
	}
}

// Unlock : unlocks a node at a specific offset
func (wl *PageLock) Unlock(offset int64, readonly bool) {
	wl.mu.Lock()

	l := wl.locks[offset]

	wl.mu.Unlock()

	if readonly {
		l.RUnlock()
	} else {
		l.Unlock()
	}
	// delete(wl.locks, offset)
}
