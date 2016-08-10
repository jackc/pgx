package pgx

import (
	"errors"
	"sync"
	"time"
)

type ConnPoolConfig struct {
	ConnConfig
	MaxConnections int               // max simultaneous connections to use, default 5, must be at least 2
	AfterConnect   func(*Conn) error // function to call on every new connection
	AcquireTimeout time.Duration     // max wait time when all connections are busy (0 means no timeout)
}

type ConnPool struct {
	allConnections       []*Conn
	availableConnections []*Conn
	cond                 *sync.Cond
	config               ConnConfig // config used when establishing connection
	inProgressConnects   int
	maxConnections       int
	resetCount           int
	afterConnect         func(*Conn) error
	logger               Logger
	logLevel             int
	closed               bool
	preparedStatements   map[string]*PreparedStatement
	acquireTimeout       time.Duration
	pgTypes              map[Oid]PgType
	pgsql_af_inet        *byte
	pgsql_af_inet6       *byte
	txAfterClose         func(tx *Tx)
	rowsAfterClose       func(rows *Rows)
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
	p.acquireTimeout = config.AcquireTimeout
	if p.acquireTimeout < 0 {
		return nil, errors.New("AcquireTimeout must be equal to or greater than 0")
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

	p.txAfterClose = func(tx *Tx) {
		p.Release(tx.Conn())
	}

	p.rowsAfterClose = func(rows *Rows) {
		p.Release(rows.Conn())
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
	c, err := p.acquire(nil)
	p.cond.L.Unlock()
	return c, err
}

// deadlinePassed returns true if the given deadline has passed.
func (p *ConnPool) deadlinePassed(deadline *time.Time) bool {
	return deadline != nil && time.Now().After(*deadline)
}

// acquire performs acquision assuming pool is already locked
func (p *ConnPool) acquire(deadline *time.Time) (*Conn, error) {
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

	// Set initial timeout/deadline value. If the method (acquire) happens to
	// recursively call itself the deadline should retain its value.
	if deadline == nil && p.acquireTimeout > 0 {
		tmp := time.Now().Add(p.acquireTimeout)
		deadline = &tmp
	}

	// Make sure the deadline (if it is) has not passed yet
	if p.deadlinePassed(deadline) {
		return nil, errors.New("Timeout: Acquire connection timeout")
	}

	// If there is a deadline then start a timeout timer
	var timer *time.Timer
	if deadline != nil {
		timer = time.AfterFunc(deadline.Sub(time.Now()), func() {
			p.cond.Broadcast()
		})
		defer timer.Stop()
	}

	// No connections are available, but we can create more
	if len(p.allConnections)+p.inProgressConnects < p.maxConnections {
		// Create a new connection.
		// Careful here: createConnectionUnlocked() removes the current lock,
		// creates a connection and then locks it back.
		if c, err := p.createConnectionUnlocked(); err == nil {
			c.poolResetCount = p.resetCount
			p.allConnections = append(p.allConnections, c)
			return c, nil
		} else {
			return nil, err
		}
	} else {
		// All connections are in use and we cannot create more
		if p.logLevel >= LogLevelWarn {
			p.logger.Warn("All connections in pool are busy - waiting...")
		}

		// Wait until there is an available connection OR room to create a new connection
		for len(p.availableConnections) == 0 && len(p.allConnections)+p.inProgressConnects == p.maxConnections {
			if p.deadlinePassed(deadline) {
				return nil, errors.New("Timeout: All connections in pool are busy")
			}
			p.cond.Wait()
		}
	}

	// Stop the timer so that we do not spawn it on every acquire call.
	if timer != nil {
		timer.Stop()
	}
	return p.acquire(deadline)
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
		p.removeFromAllConnections(conn)
	}
	p.cond.L.Unlock()
	p.cond.Signal()
}

// removeFromAllConnections Removes the given connection from the list.
// It returns true if the connection was found and removed or false otherwise.
func (p *ConnPool) removeFromAllConnections(conn *Conn) bool {
	for i, c := range p.allConnections {
		if conn == c {
			p.allConnections = append(p.allConnections[:i], p.allConnections[i+1:]...)
			return true
		}
	}
	return false
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
	c, err := connect(p.config, p.pgTypes, p.pgsql_af_inet, p.pgsql_af_inet6)
	if err != nil {
		return nil, err
	}
	return p.afterConnectionCreated(c)
}

// createConnectionUnlocked Removes the current lock, creates a new connection, and
// then locks it back.
// Here is the point: lets say our pool dialer's OpenTimeout is set to 3 seconds.
// And we have a pool with 20 connections in it, and we try to acquire them all at
// startup.
// If it happens that the remote server is not accessible, then the first connection
// in the pool blocks all the others for 3 secs, before it gets the timeout. Then
// connection #2 holds the lock and locks everything for the next 3 secs until it
// gets OpenTimeout err, etc. And the very last 20th connection will fail only after
// 3 * 20 = 60 secs.
// To avoid this we put Connect(p.config) outside of the lock (it is thread safe)
// what would allow us to make all the 20 connection in parallel (more or less).
func (p *ConnPool) createConnectionUnlocked() (*Conn, error) {
	p.inProgressConnects++
	p.cond.L.Unlock()
	c, err := Connect(p.config)
	p.cond.L.Lock()
	p.inProgressConnects--

	if err != nil {
		return nil, err
	}
	return p.afterConnectionCreated(c)
}

// afterConnectionCreated executes (if it is) afterConnect() callback and prepares
// all the known statements for the new connection.
func (p *ConnPool) afterConnectionCreated(c *Conn) (*Conn, error) {
	p.pgTypes = c.PgTypes
	p.pgsql_af_inet = c.pgsql_af_inet
	p.pgsql_af_inet6 = c.pgsql_af_inet6

	if p.afterConnect != nil {
		err := p.afterConnect(c)
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
// Query/Exec/PrepareEx without concern for if the statement has already been prepared.
func (p *ConnPool) Prepare(name, sql string) (*PreparedStatement, error) {
	return p.PrepareEx(name, sql, nil)
}

// PrepareEx creates a prepared statement on a connection in the pool to test the
// statement is valid. If it succeeds all connections accessed through the pool
// will have the statement available.
//
// PrepareEx creates a prepared statement with name and sql. sql can contain placeholders
// for bound parameters. These placeholders are referenced positional as $1, $2, etc.
// It defers from Prepare as it allows additional options (such as parameter OIDs) to be passed via struct
//
// PrepareEx is idempotent; i.e. it is safe to call PrepareEx multiple times with the same
// name and sql arguments. This allows a code path to PrepareEx and Query/Exec/Prepare without
// concern for if the statement has already been prepared.
func (p *ConnPool) PrepareEx(name, sql string, opts *PrepareExOptions) (*PreparedStatement, error) {
	p.cond.L.Lock()
	defer p.cond.L.Unlock()

	if ps, ok := p.preparedStatements[name]; ok && ps.SQL == sql {
		return ps, nil
	}

	c, err := p.acquire(nil)
	if err != nil {
		return nil, err
	}

	p.availableConnections = append(p.availableConnections, c)

	// Double check that the statement was not prepared by someone else
	// while we were acquiring the connection (since acquire is not fully
	// blocking now, see createConnectionUnlocked())
	if ps, ok := p.preparedStatements[name]; ok && ps.SQL == sql {
		return ps, nil
	}

	ps, err := c.PrepareEx(name, sql, opts)
	if err != nil {
		return nil, err
	}

	for _, c := range p.availableConnections {
		_, err := c.PrepareEx(name, sql, opts)
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
	delete(p.preparedStatements, name)

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
			}
			continue
		}

		tx.AfterClose(p.txAfterClose)
		return tx, nil
	}
}

// CopyTo acquires a connection, delegates the call to that connection, and releases the connection
func (p *ConnPool) CopyTo(tableName string, columnNames []string, rowSrc CopyToSource) (int, error) {
	c, err := p.Acquire()
	if err != nil {
		return 0, err
	}
	defer p.Release(c)

	return c.CopyTo(tableName, columnNames, rowSrc)
}
