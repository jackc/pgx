package pgx

import (
	"errors"
	"sync"
)

type ConnPoolConfig struct {
	ConnConfig
	MaxConnections int               // max simultaneous connections to use, default 5, must be at least 2
	AfterConnect   func(*Conn) error // function to call on every new connection
}

type ConnPool struct {
	allConnections       []*Conn
	availableConnections []*Conn
	cond                 *sync.Cond
	config               ConnConfig // config used when establishing connection
	maxConnections       int
	resetCount           int
	afterConnect         func(*Conn) error
	logger               Logger
	logLevel             int
	closed               bool
	preparedStatements   map[string]*PreparedStatement
}

type ConnPoolStat struct {
	MaxConnections       int // max simultaneous connections to use
	CurrentConnections   int // current live connections
	AvailableConnections int // unused live connections
}

// NewConnPool creates a new ConnPool. config.ConnConfig is passed through to
// Connect directly.
func NewConnPool(config ConnPoolConfig) (p *ConnPool, err error) {
	p = new(ConnPool)
	p.config = config.ConnConfig
	p.maxConnections = config.MaxConnections
	if p.maxConnections == 0 {
		p.maxConnections = 5
	}
	if p.maxConnections < 1 {
		return nil, errors.New("MaxConnections must be at least 1")
	}

	p.afterConnect = config.AfterConnect

	if config.LogLevel != 0 {
		p.logLevel = config.LogLevel
	} else {
		// Preserve pre-LogLevel behavior by defaulting to LogLevelDebug
		p.logLevel = LogLevelDebug
	}
	p.logger = config.Logger
	if p.logger == nil {
		p.logLevel = LogLevelNone
	}

	p.allConnections = make([]*Conn, 0, p.maxConnections)
	p.availableConnections = make([]*Conn, 0, p.maxConnections)
	p.preparedStatements = make(map[string]*PreparedStatement)
	p.cond = sync.NewCond(new(sync.Mutex))

	// Initially establish one connection
	var c *Conn
	c, err = p.createConnection()
	if err != nil {
		return
	}
	p.allConnections = append(p.allConnections, c)
	p.availableConnections = append(p.availableConnections, c)

	return
}

// Acquire takes exclusive use of a connection until it is released.
func (p *ConnPool) Acquire() (*Conn, error) {
	p.cond.L.Lock()
	c, err := p.acquire()
	p.cond.L.Unlock()
	return c, err
}

// acquire performs acquision assuming pool is already locked
func (p *ConnPool) acquire() (*Conn, error) {
	if p.closed {
		return nil, errors.New("cannot acquire from closed pool")
	}

	// A connection is available
	if len(p.availableConnections) > 0 {
		c := p.availableConnections[len(p.availableConnections)-1]
		c.poolResetCount = p.resetCount
		p.availableConnections = p.availableConnections[:len(p.availableConnections)-1]
		return c, nil
	}

	// No connections are available, but we can create more
	if len(p.allConnections) < p.maxConnections {
		c, err := p.createConnection()
		if err != nil {
			return nil, err
		}
		c.poolResetCount = p.resetCount
		p.allConnections = append(p.allConnections, c)
		return c, nil
	}

	// All connections are in use and we cannot create more
	if p.logLevel >= LogLevelWarn {
		p.logger.Warn("All connections in pool are busy - waiting...")
	}

	// Wait until there is an available connection OR room to create a new connection
	for len(p.availableConnections) == 0 && len(p.allConnections) == p.maxConnections {
		p.cond.Wait()
	}

	return p.acquire()
}

// Release gives up use of a connection.
func (p *ConnPool) Release(conn *Conn) {
	if conn.TxStatus != 'I' {
		conn.Exec("rollback")
	}

	if len(conn.channels) > 0 {
		if err := conn.Unlisten("*"); err != nil {
			conn.die(err)
		}
		conn.channels = make(map[string]struct{})
	}
	conn.notifications = nil

	p.cond.L.Lock()

	if conn.poolResetCount != p.resetCount {
		conn.Close()
		p.cond.L.Unlock()
		p.cond.Signal()
		return
	}

	if conn.IsAlive() {
		p.availableConnections = append(p.availableConnections, conn)
	} else {
		ac := p.allConnections
		for i, c := range ac {
			if conn == c {
				ac[i] = ac[len(ac)-1]
				p.allConnections = ac[0 : len(ac)-1]
				break
			}
		}
	}
	p.cond.L.Unlock()
	p.cond.Signal()
}

// Close ends the use of a connection pool. It prevents any new connections
// from being acquired, waits until all acquired connections are released,
// then closes all underlying connections.
func (p *ConnPool) Close() {
	p.cond.L.Lock()
	defer p.cond.L.Unlock()

	p.closed = true

	// Wait until all connections are released
	if len(p.availableConnections) != len(p.allConnections) {
		for len(p.availableConnections) != len(p.allConnections) {
			p.cond.Wait()
		}
	}

	for _, c := range p.allConnections {
		_ = c.Close()
	}
}

// Reset closes all open connections, but leaves the pool open. It is intended
// for use when an error is detected that would disrupt all connections (such as
// a network interruption or a server state change).
//
// It is safe to reset a pool while connections are checked out. Those
// connections will be closed when they are returned to the pool.
func (p *ConnPool) Reset() {
	p.cond.L.Lock()
	defer p.cond.L.Unlock()

	p.resetCount++
	p.allConnections = make([]*Conn, 0, p.maxConnections)
	p.availableConnections = make([]*Conn, 0, p.maxConnections)
}

// invalidateAcquired causes all acquired connections to be closed when released.
// The pool must already be locked.
func (p *ConnPool) invalidateAcquired() {
	p.resetCount++

	for _, c := range p.availableConnections {
		c.poolResetCount = p.resetCount
	}

	p.allConnections = p.allConnections[:len(p.availableConnections)]
	copy(p.allConnections, p.availableConnections)
}

// Stat returns connection pool statistics
func (p *ConnPool) Stat() (s ConnPoolStat) {
	p.cond.L.Lock()
	defer p.cond.L.Unlock()

	s.MaxConnections = p.maxConnections
	s.CurrentConnections = len(p.allConnections)
	s.AvailableConnections = len(p.availableConnections)
	return
}

func (p *ConnPool) createConnection() (*Conn, error) {
	c, err := Connect(p.config)
	if err != nil {
		return nil, err
	}

	if p.afterConnect != nil {
		err = p.afterConnect(c)
		if err != nil {
			c.die(err)
			return nil, err
		}
	}

	for _, ps := range p.preparedStatements {
		if _, err := c.Prepare(ps.Name, ps.SQL); err != nil {
			c.die(err)
			return nil, err
		}
	}

	return c, nil
}

// Exec acquires a connection, delegates the call to that connection, and releases the connection
func (p *ConnPool) Exec(sql string, arguments ...interface{}) (commandTag CommandTag, err error) {
	var c *Conn
	if c, err = p.Acquire(); err != nil {
		return
	}
	defer p.Release(c)

	return c.Exec(sql, arguments...)
}

// Query acquires a connection and delegates the call to that connection. When
// *Rows are closed, the connection is released automatically.
func (p *ConnPool) Query(sql string, args ...interface{}) (*Rows, error) {
	c, err := p.Acquire()
	if err != nil {
		// Because checking for errors can be deferred to the *Rows, build one with the error
		return &Rows{closed: true, err: err}, err
	}

	rows, err := c.Query(sql, args...)
	if err != nil {
		p.Release(c)
		return rows, err
	}

	rows.AfterClose(p.rowsAfterClose)

	return rows, nil
}

// QueryRow acquires a connection and delegates the call to that connection. The
// connection is released automatically after Scan is called on the returned
// *Row.
func (p *ConnPool) QueryRow(sql string, args ...interface{}) *Row {
	rows, _ := p.Query(sql, args...)
	return (*Row)(rows)
}

// Begin acquires a connection and begins a transaction on it. When the
// transaction is closed the connection will be automatically released.
func (p *ConnPool) Begin() (*Tx, error) {
	return p.BeginIso("")
}

// Prepare creates a prepared statement on a connection in the pool to test the
// statement is valid. If it succeeds all connections accessed through the pool
// will have the statement available.
//
// Prepare creates a prepared statement with name and sql. sql can contain
// placeholders for bound parameters. These placeholders are referenced
// positional as $1, $2, etc.
//
// Prepare is idempotent; i.e. it is safe to call Prepare multiple times with
// the same name and sql arguments. This allows a code path to Prepare and
// Query/Exec without concern for if the statement has already been prepared.
func (p *ConnPool) Prepare(name, sql string) (*PreparedStatement, error) {
	p.cond.L.Lock()
	defer p.cond.L.Unlock()

	if ps, ok := p.preparedStatements[name]; ok && ps.SQL == sql {
		return ps, nil
	}

	c, err := p.acquire()
	if err != nil {
		return nil, err
	}
	ps, err := c.Prepare(name, sql)
	p.availableConnections = append(p.availableConnections, c)
	if err != nil {
		return nil, err
	}

	for _, c := range p.availableConnections {
		_, err := c.Prepare(name, sql)
		if err != nil {
			return nil, err
		}
	}

	p.invalidateAcquired()
	p.preparedStatements[name] = ps

	return ps, err
}

// Deallocate releases a prepared statement from all connections in the pool.
func (p *ConnPool) Deallocate(name string) (err error) {
	p.cond.L.Lock()
	defer p.cond.L.Unlock()

	for _, c := range p.availableConnections {
		if err := c.Deallocate(name); err != nil {
			return err
		}
	}

	p.invalidateAcquired()

	return nil
}

// BeginIso acquires a connection and begins a transaction in isolation mode iso
// on it. When the transaction is closed the connection will be automatically
// released.
func (p *ConnPool) BeginIso(iso string) (*Tx, error) {
	for {
		c, err := p.Acquire()
		if err != nil {
			return nil, err
		}

		tx, err := c.BeginIso(iso)
		if err != nil {
			alive := c.IsAlive()
			p.Release(c)

			// If connection is still alive then the error is not something trying
			// again on a new connection would fix, so just return the error. But
			// if the connection is dead try to acquire a new connection and try
			// again.
			if alive {
				return nil, err
			} else {
				continue
			}
		}

		tx.AfterClose(p.txAfterClose)
		return tx, nil
	}
}

func (p *ConnPool) txAfterClose(tx *Tx) {
	p.Release(tx.Conn())
}

func (p *ConnPool) rowsAfterClose(rows *Rows) {
	p.Release(rows.Conn())
}
