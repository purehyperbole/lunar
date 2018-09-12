package table

import (
	"sync"
)

// PageLock : handles locking for index nodes
type PageLock struct {
	mu    sync.Mutex
	locks map[int64]*sync.Mutex
}

// NewPageLock : creates a new page lock for coordinating reads and writes to db index
func NewPageLock() *PageLock {
	return &PageLock{
		locks: make(map[int64]*sync.Mutex),
	}
}

// Lock : locks a node at a specific offset
func (wl *PageLock) Lock(offset int64) {
	wl.mu.Lock()

	if wl.locks[offset] == nil {
		wl.locks[offset] = &sync.Mutex{}
	}

	wl.mu.Unlock()

	wl.locks[offset].Lock()
}

// Unlock : unlocks a node at a specific offset
func (wl *PageLock) Unlock(offset int64) {
	wl.mu.Lock()
	defer wl.mu.Unlock()

	wl.locks[offset].Unlock()
	// delete(wl.locks, offset)
}
