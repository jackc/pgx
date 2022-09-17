package stmtcache

import (
	"container/list"

	"github.com/jackc/pgx/v5/pgconn"
)

// LRUCache implements Cache with a Least Recently Used (LRU) cache.
type LRUCache struct {
	cap          int
	m            map[string]*list.Element
	l            *list.List
	invalidStmts []*pgconn.StatementDescription
}

// NewLRUCache creates a new LRUCache. cap is the maximum size of the cache.
func NewLRUCache(cap int) *LRUCache {
	return &LRUCache{
		cap: cap,
		m:   make(map[string]*list.Element),
		l:   list.New(),
	}
}

// Get returns the statement description for sql. Returns nil if not found.
func (c *LRUCache) Get(key string) *pgconn.StatementDescription {
	if el, ok := c.m[key]; ok {
		c.l.MoveToFront(el)
		return el.Value.(*pgconn.StatementDescription)
	}

	return nil

}

// Put stores sd in the cache. Put panics if sd.SQL is "". Put does nothing if sd.SQL already exists in the cache.
func (c *LRUCache) Put(sd *pgconn.StatementDescription) {
	if sd.SQL == "" {
		panic("cannot store statement description with empty SQL")
	}

	if _, present := c.m[sd.SQL]; present {
		return
	}

	if c.l.Len() == c.cap {
		c.invalidateOldest()
	}

	el := c.l.PushFront(sd)
	c.m[sd.SQL] = el
}

// Invalidate invalidates statement description identified by sql. Does nothing if not found.
func (c *LRUCache) Invalidate(sql string) {
	if el, ok := c.m[sql]; ok {
		delete(c.m, sql)
		c.invalidStmts = append(c.invalidStmts, el.Value.(*pgconn.StatementDescription))
		c.l.Remove(el)
	}
}

// InvalidateAll invalidates all statement descriptions.
func (c *LRUCache) InvalidateAll() {
	el := c.l.Front()
	for el != nil {
		c.invalidStmts = append(c.invalidStmts, el.Value.(*pgconn.StatementDescription))
		el = el.Next()
	}

	c.m = make(map[string]*list.Element)
	c.l = list.New()
}

func (c *LRUCache) HandleInvalidated() []*pgconn.StatementDescription {
	invalidStmts := c.invalidStmts
	c.invalidStmts = nil
	return invalidStmts
}

// Len returns the number of cached prepared statement descriptions.
func (c *LRUCache) Len() int {
	return c.l.Len()
}

// Cap returns the maximum number of cached prepared statement descriptions.
func (c *LRUCache) Cap() int {
	return c.cap
}

func (c *LRUCache) invalidateOldest() {
	oldest := c.l.Back()
	sd := oldest.Value.(*pgconn.StatementDescription)
	c.invalidStmts = append(c.invalidStmts, sd)
	delete(c.m, sd.SQL)
	c.l.Remove(oldest)
}
