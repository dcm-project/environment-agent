// Package routing implements resource operation routing for the environment agent.
package routing

import (
	"container/list"
	"sync"
)

// DenyList is an LRU-evicting set of resourceId values from cancel CEs.
type DenyList struct {
	maxSize int
	mu      sync.Mutex
	items   map[string]*list.Element
	order   *list.List
}

// NewDenyList creates a deny list with the given maximum capacity.
// If maxSize <= 0, it defaults to 1 to guarantee safe LRU operation.
func NewDenyList(maxSize int) *DenyList {
	if maxSize <= 0 {
		maxSize = 1
	}
	return &DenyList{
		maxSize: maxSize,
		items:   make(map[string]*list.Element),
		order:   list.New(),
	}
}

// Add inserts a resourceId into the deny list. If at capacity, evicts the LRU entry.
func (d *DenyList) Add(resourceID string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if elem, ok := d.items[resourceID]; ok {
		d.order.MoveToFront(elem)
		return
	}
	if len(d.items) >= d.maxSize {
		back := d.order.Back()
		d.order.Remove(back)
		delete(d.items, back.Value.(string))
	}
	elem := d.order.PushFront(resourceID)
	d.items[resourceID] = elem
}

// AddIfAbsent atomically inserts resourceID if not already present.
// Returns true if the entry was newly added, false if it already existed.
func (d *DenyList) AddIfAbsent(resourceID string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	if elem, ok := d.items[resourceID]; ok {
		d.order.MoveToFront(elem)
		return false
	}
	if len(d.items) >= d.maxSize {
		back := d.order.Back()
		d.order.Remove(back)
		delete(d.items, back.Value.(string))
	}
	d.items[resourceID] = d.order.PushFront(resourceID)
	return true
}

// Contains checks whether a resourceId is in the deny list without consuming it.
// Access refreshes the LRU position.
func (d *DenyList) Contains(resourceID string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	elem, ok := d.items[resourceID]
	if !ok {
		return false
	}
	d.order.MoveToFront(elem)
	return true
}

// Consume checks and removes a resourceId from the deny list atomically.
// Returns true if the entry was present (and is now removed).
func (d *DenyList) Consume(resourceID string) bool {
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

// Remove deletes a resourceId from the deny list without returning whether it was present.
func (d *DenyList) Remove(resourceID string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	elem, ok := d.items[resourceID]
	if !ok {
		return
	}
	d.order.Remove(elem)
	delete(d.items, resourceID)
}

// Len returns the number of entries in the deny list.
func (d *DenyList) Len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.items)
}
