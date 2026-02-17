package stmtcache

import (
	"github.com/jackc/pgx/v5/pgconn"
)

// lruNode is a typed doubly-linked list node with freelist support.
type lruNode struct {
	sd   *pgconn.StatementDescription
	prev *lruNode
	next *lruNode
}

// LRUCache implements Cache with a Least Recently Used (LRU) cache.
type LRUCache struct {
	m    map[string]*lruNode
	head *lruNode

	tail     *lruNode
	len      int
	cap      int
	freelist *lruNode

	invalidStmts []*pgconn.StatementDescription
	invalidSet   map[string]struct{}
}

// NewLRUCache creates a new LRUCache. cap is the maximum size of the cache.
func NewLRUCache(cap int) *LRUCache {
	head := &lruNode{}
	tail := &lruNode{}
	head.next = tail
	tail.prev = head

	return &LRUCache{
		cap:        cap,
		m:          make(map[string]*lruNode, cap),
		head:       head,
		tail:       tail,
		invalidSet: make(map[string]struct{}),
	}
}

// Get returns the statement description for sql. Returns nil if not found.
func (c *LRUCache) Get(key string) *pgconn.StatementDescription {
	node, ok := c.m[key]
	if !ok {
		return nil
	}
	c.moveToFront(node)
	return node.sd
}

// Put stores sd in the cache. Put panics if sd.SQL is "". Put does nothing if sd.SQL already exists in the cache or
// sd.SQL has been invalidated and HandleInvalidated has not been called yet.
func (c *LRUCache) Put(sd *pgconn.StatementDescription) {
	if sd.SQL == "" {
		panic("cannot store statement description with empty SQL")
	}

	if _, present := c.m[sd.SQL]; present {
		return
	}

	// The statement may have been invalidated but not yet handled. Do not readd it to the cache.
	if _, invalidated := c.invalidSet[sd.SQL]; invalidated {
		return
	}

	if c.len == c.cap {
		c.invalidateOldest()
	}

	node := c.allocNode()
	node.sd = sd
	c.insertAfter(c.head, node)
	c.m[sd.SQL] = node
	c.len++
}

// Invalidate invalidates statement description identified by sql. Does nothing if not found.
func (c *LRUCache) Invalidate(sql string) {
	node, ok := c.m[sql]
	if !ok {
		return
	}
	delete(c.m, sql)
	c.invalidStmts = append(c.invalidStmts, node.sd)
	c.invalidSet[sql] = struct{}{}
	c.unlink(node)
	c.len--
	c.freeNode(node)
}

// InvalidateAll invalidates all statement descriptions.
func (c *LRUCache) InvalidateAll() {
	for node := c.head.next; node != c.tail; {
		next := node.next
		c.invalidStmts = append(c.invalidStmts, node.sd)
		c.invalidSet[node.sd.SQL] = struct{}{}
		c.freeNode(node)
		node = next
	}

	clear(c.m)
	c.head.next = c.tail
	c.tail.prev = c.head
	c.len = 0
}

// GetInvalidated returns a slice of all statement descriptions invalidated since the last call to RemoveInvalidated.
func (c *LRUCache) GetInvalidated() []*pgconn.StatementDescription {
	return c.invalidStmts
}

// RemoveInvalidated removes all invalidated statement descriptions. No other calls to Cache must be made between a
// call to GetInvalidated and RemoveInvalidated or RemoveInvalidated may remove statement descriptions that were
// never seen by the call to GetInvalidated.
func (c *LRUCache) RemoveInvalidated() {
	c.invalidStmts = c.invalidStmts[:0]
	clear(c.invalidSet)
}

// Len returns the number of cached prepared statement descriptions.
func (c *LRUCache) Len() int {
	return c.len
}

// Cap returns the maximum number of cached prepared statement descriptions.
func (c *LRUCache) Cap() int {
	return c.cap
}

func (c *LRUCache) invalidateOldest() {
	node := c.tail.prev
	if node == c.head {
		return
	}
	c.invalidStmts = append(c.invalidStmts, node.sd)
	c.invalidSet[node.sd.SQL] = struct{}{}
	delete(c.m, node.sd.SQL)
	c.unlink(node)
	c.len--
	c.freeNode(node)
}

// List operations - sentinel nodes eliminate nil checks

func (c *LRUCache) insertAfter(at, node *lruNode) {
	node.prev = at
	node.next = at.next
	at.next.prev = node
	at.next = node
}

func (c *LRUCache) unlink(node *lruNode) {
	node.prev.next = node.next
	node.next.prev = node.prev
}

func (c *LRUCache) moveToFront(node *lruNode) {
	if node.prev == c.head {
		return
	}
	c.unlink(node)
	c.insertAfter(c.head, node)
}

// Node pool operations - reuse evicted nodes to avoid allocations

func (c *LRUCache) allocNode() *lruNode {
	if c.freelist != nil {
		node := c.freelist
		c.freelist = node.next
		node.next = nil
		node.prev = nil
		return node
	}
	return &lruNode{}
}

func (c *LRUCache) freeNode(node *lruNode) {
	node.sd = nil
	node.prev = nil
	node.next = c.freelist
	c.freelist = node
}
