// Package lru provides a small bounded cache.
//
// Entry-bounded rather than byte-bounded, deliberately: everything cached here
// is a filtered state view, which is a handful of entries. gopro-worker bounds
// its caches in megabytes because it caches *whole* state maps, where one
// entry can be tens of megabytes. That does not apply to anything here, and an
// entry bound is simpler to reason about.
package lru

import (
	"container/list"
	"sync"
)

// Cache is a bounded, concurrency-safe LRU.
type Cache[K comparable, V any] struct {
	mu    sync.Mutex
	max   int
	items map[K]*list.Element
	order *list.List // front = most recently used
}

type entry[K comparable, V any] struct {
	key   K
	value V
}

// New returns a cache holding at most max entries. A max of zero or less
// disables caching entirely, which is a supported configuration rather than a
// mistake: it makes a "does the cache hide a bug?" question answerable.
func New[K comparable, V any](max int) *Cache[K, V] {
	return &Cache[K, V]{
		max:   max,
		items: make(map[K]*list.Element),
		order: list.New(),
	}
}

// Get returns the value for a key.
func (c *Cache[K, V]) Get(key K) (V, bool) {
	var zero V
	if c == nil || c.max <= 0 {
		return zero, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	elem, ok := c.items[key]
	if !ok {
		return zero, false
	}
	c.order.MoveToFront(elem)
	return elem.Value.(*entry[K, V]).value, true
}

// Add stores a value, evicting the least recently used entry if needed.
func (c *Cache[K, V]) Add(key K, value V) {
	if c == nil || c.max <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.items[key]; ok {
		elem.Value.(*entry[K, V]).value = value
		c.order.MoveToFront(elem)
		return
	}
	c.items[key] = c.order.PushFront(&entry[K, V]{key: key, value: value})
	for c.order.Len() > c.max {
		oldest := c.order.Back()
		if oldest == nil {
			break
		}
		c.order.Remove(oldest)
		delete(c.items, oldest.Value.(*entry[K, V]).key)
	}
}

// Purge empties the cache.
//
// Everything cached here is derived from immutable data -- a state group's
// contents are fixed once written -- so this is not needed for correctness on
// the write path. It exists for the replication subscriber to call when it
// loses its connection and can no longer see rooms being purged or events
// deleted underneath us.
func (c *Cache[K, V]) Purge() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items = make(map[K]*list.Element)
	c.order.Init()
}

// Len reports how many entries are held.
func (c *Cache[K, V]) Len() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.order.Len()
}
