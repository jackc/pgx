package pscache

import (
	"container/list"
	"context"
	"fmt"
	"sync/atomic"

	"github.com/jackc/pgconn"
)

var lruCacheCount uint64

// LRUCache implements cache with a Least Recently Used (LRU) cache.
type LRUCache struct {
	conn         *pgconn.PgConn
	mode         int
	cap          int
	prepareCount int
	m            map[string]*list.Element
	l            *list.List
	psNamePrefix string
}

// NewLRUCache creates a new LRUCache. mode is either PrepareMode or DescribeMode. cap is the maximum size of the cache.
func NewLRUCache(conn *pgconn.PgConn, mode int, cap int) *LRUCache {
	mustBeValidMode(mode)
	mustBeValidCap(cap)

	n := atomic.AddUint64(&lruCacheCount, 1)

	return &LRUCache{
		conn:         conn,
		mode:         mode,
		cap:          cap,
		m:            make(map[string]*list.Element),
		l:            list.New(),
		psNamePrefix: fmt.Sprintf("lrupsc_%d", n),
	}
}

// Get returns the prepared statement description for sql preparing or describing the sql on the server as needed.
func (c *LRUCache) Get(ctx context.Context, sql string) (*pgconn.PreparedStatementDescription, error) {
	if el, ok := c.m[sql]; ok {
		c.l.MoveToFront(el)
		return el.Value.(*pgconn.PreparedStatementDescription), nil
	}

	if c.l.Len() == c.cap {
		err := c.removeOldest(ctx)
		if err != nil {
			return nil, err
		}
	}

	psd, err := c.prepare(ctx, sql)
	if err != nil {
		return nil, err
	}

	el := c.l.PushFront(psd)
	c.m[sql] = el

	return psd, nil
}

// Clear removes all entries in the cache. Any prepared statements will be deallocated from the PostgreSQL session.
func (c *LRUCache) Clear(ctx context.Context) error {
	for c.l.Len() > 0 {
		err := c.removeOldest(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

// Len returns the number of cached prepared statement descriptions.
func (c *LRUCache) Len() int {
	return c.l.Len()
}

// Cap returns the maximum number of cached prepared statement descriptions.
func (c *LRUCache) Cap() int {
	return c.cap
}

// Mode returns the mode of the cache (PrepareMode or DescribeMode)
func (c *LRUCache) Mode() int {
	return c.mode
}

func (c *LRUCache) prepare(ctx context.Context, sql string) (*pgconn.PreparedStatementDescription, error) {
	var name string
	if c.mode == PrepareMode {
		name = fmt.Sprintf("%s_%d", c.psNamePrefix, c.prepareCount)
		c.prepareCount += 1
	}

	return c.conn.Prepare(ctx, name, sql, nil)
}

func (c *LRUCache) removeOldest(ctx context.Context) error {
	oldest := c.l.Back()
	c.l.Remove(oldest)
	if c.mode == PrepareMode {
		return c.conn.Exec(ctx, fmt.Sprintf("deallocate %s", oldest.Value.(*pgconn.PreparedStatementDescription).Name)).Close()
	}
	return nil
}
