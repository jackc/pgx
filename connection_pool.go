package pgx

import (
	"io"
	"sync"
)

type ConnectionPoolOptions struct {
	MaxConnections int // max simultaneous connections to use
	AfterConnect   func(*Connection) error
	Logger         Logger
}

type ConnectionPool struct {
	allConnections       []*Connection
	availableConnections []*Connection
	cond                 *sync.Cond
	parameters           ConnectionParameters // parameters used when establishing connection
	maxConnections       int
	afterConnect         func(*Connection) error
	logger               Logger
}

type ConnectionPoolStat struct {
	MaxConnections       int // max simultaneous connections to use
	CurrentConnections   int // current live connections
	AvailableConnections int // unused live connections
}

// NewConnectionPool creates a new ConnectionPool. parameters are passed through to
// Connect directly.
func NewConnectionPool(parameters ConnectionParameters, options ConnectionPoolOptions) (p *ConnectionPool, err error) {
	p = new(ConnectionPool)
	p.parameters = parameters
	p.maxConnections = options.MaxConnections
	p.afterConnect = options.AfterConnect
	if options.Logger != nil {
		p.logger = options.Logger
	} else {
		p.logger = nullLogger("null")
	}

	p.allConnections = make([]*Connection, 0, p.maxConnections)
	p.availableConnections = make([]*Connection, 0, p.maxConnections)
	p.cond = sync.NewCond(new(sync.Mutex))

	// Initially establish one connection
	var c *Connection
	c, err = p.createConnection()
	if err != nil {
		return
	}
	p.allConnections = append(p.allConnections, c)
	p.availableConnections = append(p.availableConnections, c)

	return
}

// Acquire takes exclusive use of a connection until it is released.
func (p *ConnectionPool) Acquire() (c *Connection, err error) {
	p.cond.L.Lock()
	defer p.cond.L.Unlock()

	// A connection is available
	if len(p.availableConnections) > 0 {
		c = p.availableConnections[len(p.availableConnections)-1]
		p.availableConnections = p.availableConnections[:len(p.availableConnections)-1]
		return
	}

	// No connections are available, but we can create more
	if len(p.allConnections) < p.maxConnections {
		c, err = p.createConnection()
		if err != nil {
			return
		}
		p.allConnections = append(p.allConnections, c)
		return
	}

	// All connections are in use and we cannot create more
	if len(p.availableConnections) == 0 {
		p.logger.Warning("All connections in pool are busy - waiting...")
		for len(p.availableConnections) == 0 {
			p.cond.Wait()
		}
	}

	c = p.availableConnections[len(p.availableConnections)-1]
	p.availableConnections = p.availableConnections[:len(p.availableConnections)-1]

	return
}

// Release gives up use of a connection.
func (p *ConnectionPool) Release(conn *Connection) {
	if conn.TxStatus != 'I' {
		conn.Execute("rollback")
	}

	p.cond.L.Lock()
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

// Close ends the use of a connection by closing all underlying connections.
func (p *ConnectionPool) Close() {
	for i := 0; i < p.maxConnections; i++ {
		if c, err := p.Acquire(); err != nil {
			_ = c.Close()
		}
	}
}

func (p *ConnectionPool) Stat() (s ConnectionPoolStat) {
	p.cond.L.Lock()
	defer p.cond.L.Unlock()

	s.MaxConnections = p.maxConnections
	s.CurrentConnections = len(p.allConnections)
	s.AvailableConnections = len(p.availableConnections)
	return
}

func (p *ConnectionPool) MaxConnectionCount() int {
	return p.maxConnections
}

func (p *ConnectionPool) CurrentConnectionCount() int {
	return p.maxConnections
}

func (p *ConnectionPool) createConnection() (c *Connection, err error) {
	c, err = Connect(p.parameters)
	if err != nil {
		return
	}
	if p.afterConnect != nil {
		err = p.afterConnect(c)
		if err != nil {
			return
		}
	}
	return
}

// SelectFunc acquires a connection, delegates the call to that connection, and releases the connection
func (p *ConnectionPool) SelectFunc(sql string, onDataRow func(*DataRowReader) error, arguments ...interface{}) (err error) {
	var c *Connection
	if c, err = p.Acquire(); err != nil {
		return
	}
	defer p.Release(c)

	return c.SelectFunc(sql, onDataRow, arguments...)
}

// SelectRows acquires a connection, delegates the call to that connection, and releases the connection
func (p *ConnectionPool) SelectRows(sql string, arguments ...interface{}) (rows []map[string]interface{}, err error) {
	var c *Connection
	if c, err = p.Acquire(); err != nil {
		return
	}
	defer p.Release(c)

	return c.SelectRows(sql, arguments...)
}

// SelectRow acquires a connection, delegates the call to that connection, and releases the connection
func (p *ConnectionPool) SelectRow(sql string, arguments ...interface{}) (row map[string]interface{}, err error) {
	var c *Connection
	if c, err = p.Acquire(); err != nil {
		return
	}
	defer p.Release(c)

	return c.SelectRow(sql, arguments...)
}

// SelectValue acquires a connection, delegates the call to that connection, and releases the connection
func (p *ConnectionPool) SelectValue(sql string, arguments ...interface{}) (v interface{}, err error) {
	var c *Connection
	if c, err = p.Acquire(); err != nil {
		return
	}
	defer p.Release(c)

	return c.SelectValue(sql, arguments...)
}

// SelectValueTo acquires a connection, delegates the call to that connection, and releases the connection
func (p *ConnectionPool) SelectValueTo(w io.Writer, sql string, arguments ...interface{}) (err error) {
	var c *Connection
	if c, err = p.Acquire(); err != nil {
		return
	}
	defer p.Release(c)

	return c.SelectValueTo(w, sql, arguments...)
}

// SelectValues acquires a connection, delegates the call to that connection, and releases the connection
func (p *ConnectionPool) SelectValues(sql string, arguments ...interface{}) (values []interface{}, err error) {
	var c *Connection
	if c, err = p.Acquire(); err != nil {
		return
	}
	defer p.Release(c)

	return c.SelectValues(sql, arguments...)
}

// Execute acquires a connection, delegates the call to that connection, and releases the connection
func (p *ConnectionPool) Execute(sql string, arguments ...interface{}) (commandTag string, err error) {
	var c *Connection
	if c, err = p.Acquire(); err != nil {
		return
	}
	defer p.Release(c)

	return c.Execute(sql, arguments...)
}

// Transaction acquires a connection, delegates the call to that connection,
// and releases the connection. The call signature differs slightly from the
// underlying Transaction in that the callback function accepts a *Connection
func (p *ConnectionPool) Transaction(f func(conn *Connection) bool) (committed bool, err error) {
	var c *Connection
	if c, err = p.Acquire(); err != nil {
		return
	}
	defer p.Release(c)

	return c.Transaction(func() bool {
		return f(c)
	})
}

// TransactionIso acquires a connection, delegates the call to that connection,
// and releases the connection. The call signature differs slightly from the
// underlying TransactionIso in that the callback function accepts a *Connection
func (p *ConnectionPool) TransactionIso(isoLevel string, f func(conn *Connection) bool) (committed bool, err error) {
	var c *Connection
	if c, err = p.Acquire(); err != nil {
		return
	}
	defer p.Release(c)

	return c.TransactionIso(isoLevel, func() bool {
		return f(c)
	})
}
