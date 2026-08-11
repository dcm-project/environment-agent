package routing

import (
	"container/list"
	"sync"
)

// ResourceSet is a concurrent set of resource IDs. By default it is
// LRU-evicting (bounded); see NewUnboundedResourceSet for the non-evicting
// variant.
type ResourceSet struct {
	maxSize   int
	unbounded bool
	mu        sync.Mutex
	items     map[string]*list.Element
	order     *list.List
}

// NewResourceSet creates an LRU-evicting resource set with the given maximum
// capacity. If maxSize <= 0, it defaults to 1 to guarantee safe LRU operation.
// Only appropriate for ledgers that can tolerate graceful degradation under
// eviction (e.g. a cancel-rejection ledger); never use this for a mutual-
// exclusion lock — see NewUnboundedResourceSet.
func NewResourceSet(maxSize int) *ResourceSet {
	if maxSize <= 0 {
		maxSize = 1
	}
	return &ResourceSet{
		maxSize: maxSize,
		items:   make(map[string]*list.Element),
		order:   list.New(),
	}
}

// NewUnboundedResourceSet creates a resource set that never evicts entries.
// Required for mutual-exclusion use cases (e.g. the transient in-flight
// forward lock, F13) where entries are always explicitly removed by the
// holder and eviction would silently release a lock still in use, allowing
// double-dispatch. Safe to leave unbounded because such sets are naturally
// bounded by the number of concurrently in-progress operations, not by total
// resource cardinality.
func NewUnboundedResourceSet() *ResourceSet {
	return &ResourceSet{
		unbounded: true,
		items:     make(map[string]*list.Element),
		order:     list.New(),
	}
}

// Add inserts a resourceId into the set. If at capacity, evicts the LRU entry.
func (d *ResourceSet) Add(resourceID string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.insert(resourceID)
}

// AddIfAbsent atomically inserts resourceID if not already present.
// Returns true if the entry was newly added, false if it already existed.
func (d *ResourceSet) AddIfAbsent(resourceID string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.insert(resourceID)
}

// insert adds resourceID to the set, evicting the LRU entry if at capacity.
// Returns true if the entry was newly added. Caller must hold d.mu.
func (d *ResourceSet) insert(resourceID string) bool {
	if elem, ok := d.items[resourceID]; ok {
		d.order.MoveToFront(elem)
		return false
	}
	if !d.unbounded && len(d.items) >= d.maxSize {
		back := d.order.Back()
		d.order.Remove(back)
		delete(d.items, back.Value.(string))
	}
	d.items[resourceID] = d.order.PushFront(resourceID)
	return true
}

// Contains checks whether a resourceId is in the set without consuming it.
// Access refreshes the LRU position.
func (d *ResourceSet) Contains(resourceID string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	elem, ok := d.items[resourceID]
	if !ok {
		return false
	}
	d.order.MoveToFront(elem)
	return true
}

// Consume checks and removes a resourceId from the set atomically.
// Returns true if the entry was present (and is now removed).
func (d *ResourceSet) Consume(resourceID string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	elem, ok := d.items[resourceID]
	if !ok {
		return false
	}
	d.order.Remove(elem)
	delete(d.items, resourceID)
	return true
}

// Remove deletes a resourceId from the set without returning whether it was present.
func (d *ResourceSet) Remove(resourceID string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	elem, ok := d.items[resourceID]
	if !ok {
		return
	}
	d.order.Remove(elem)
	delete(d.items, resourceID)
}

// Len returns the number of entries in the set.
func (d *ResourceSet) Len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.items)
}
