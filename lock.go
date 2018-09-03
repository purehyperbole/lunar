package lunar

import "sync"

// WriteLock : handles locking for index nodes
type WriteLock struct {
	mu    sync.Mutex
	locks map[int64]*sync.Mutex
}

// NewWriteLock : creates a new writelock for coordinating writes to db index
func NewWriteLock() *WriteLock {
	return &WriteLock{
		locks: make(map[int64]*sync.Mutex),
	}
}

// Lock : locks a node at a specific offset
func (wl *WriteLock) Lock(offset int64) {
	wl.mu.Lock()

	if wl.locks[offset] == nil {
		wl.locks[offset] = &sync.Mutex{}
	}

	wl.mu.Unlock()

	wl.locks[offset].Lock()
}

// Unlock : unlocks a node at a specific offset
func (wl *WriteLock) Unlock(offset int64) {
	wl.mu.Lock()
	defer wl.mu.Unlock()

	wl.locks[offset].Unlock()
	// delete(wl.locks, offset)
}
