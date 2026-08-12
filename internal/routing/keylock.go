package routing

import "sync"

// KeyLock is a concurrent, non-evicting set of resource IDs used purely for
// mutual exclusion (F13's transient in-flight forward lock, shared between
// Router and retry.Processor). Unlike ResourceSet, it has no LRU/eviction
// behavior: entries are always explicitly removed by the holder, and
// eviction would silently release a lock still in use, allowing
// double-dispatch. Naturally bounded by the number of concurrently
// in-progress operations, not by total resource cardinality.
type KeyLock struct {
	mu    sync.Mutex
	items map[string]struct{}
}

// NewKeyLock creates an empty KeyLock.
func NewKeyLock() *KeyLock {
	return &KeyLock{items: make(map[string]struct{})}
}

// AddIfAbsent atomically inserts resourceID if not already present.
// Returns true if the entry was newly added, false if it already existed.
func (k *KeyLock) AddIfAbsent(resourceID string) bool {
	k.mu.Lock()
	defer k.mu.Unlock()
	if _, ok := k.items[resourceID]; ok {
		return false
	}
	k.items[resourceID] = struct{}{}
	return true
}

// Remove deletes a resourceId from the set without returning whether it was present.
func (k *KeyLock) Remove(resourceID string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	delete(k.items, resourceID)
}

// Len returns the number of entries currently held.
func (k *KeyLock) Len() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return len(k.items)
}
