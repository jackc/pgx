package stmtcache

import (
	"container/list"
	"context"
	"fmt"
	"sync/atomic"

	"github.com/jackc/pgconn"
)

var lruCount uint64

// LRU implements Cache with a Least Recently Used (LRU) cache.
type LRU struct {
	conn         *pgconn.PgConn
	mode         int
	cap          int
	prepareCount int
	m            map[string]*list.Element
	l            *list.List
	psNamePrefix string
}

// NewLRU creates a new LRU. mode is either ModePrepare or ModeDescribe. cap is the maximum size of the cache.
func NewLRU(conn *pgconn.PgConn, mode int, cap int) *LRU {
	mustBeValidMode(mode)
	mustBeValidCap(cap)

	n := atomic.AddUint64(&lruCount, 1)

	return &LRU{
		conn:         conn,
		mode:         mode,
		cap:          cap,
		m:            make(map[string]*list.Element),
		l:            list.New(),
		psNamePrefix: fmt.Sprintf("lrupsc_%d", n),
	}
}

// Get returns the prepared statement description for sql preparing or describing the sql on the server as needed.
func (c *LRU) Get(ctx context.Context, sql string) (*pgconn.StatementDescription, error) {
	if el, ok := c.m[sql]; ok {
		c.l.MoveToFront(el)
		return el.Value.(*pgconn.StatementDescription), nil
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
func (c *LRU) Clear(ctx context.Context) error {
	for c.l.Len() > 0 {
		err := c.removeOldest(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

// Len returns the number of cached prepared statement descriptions.
func (c *LRU) Len() int {
	return c.l.Len()
}

// Cap returns the maximum number of cached prepared statement descriptions.
func (c *LRU) Cap() int {
	return c.cap
}

// Mode returns the mode of the cache (ModePrepare or ModeDescribe)
func (c *LRU) Mode() int {
	return c.mode
}

func (c *LRU) prepare(ctx context.Context, sql string) (*pgconn.StatementDescription, error) {
	var name string
	if c.mode == ModePrepare {
		name = fmt.Sprintf("%s_%d", c.psNamePrefix, c.prepareCount)
		c.prepareCount += 1
	}

	return c.conn.Prepare(ctx, name, sql, nil)
}

func (c *LRU) removeOldest(ctx context.Context) error {
	oldest := c.l.Back()
	c.l.Remove(oldest)
	psd := oldest.Value.(*pgconn.StatementDescription)
	delete(c.m, psd.SQL)
	if c.mode == ModePrepare {
		return c.conn.Exec(ctx, fmt.Sprintf("deallocate %s", psd.Name)).Close()
	}
	return nil
}
