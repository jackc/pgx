package pgx

type ConnectionPool struct {
	connectionChannel chan *Connection
	parameters        ConnectionParameters // options used when establishing connection
	MaxConnections    int
}

// options: options used by Connect
// MaxConnections: max simultaneous connections to use (currently all are immediately connected)
func NewConnectionPool(parameters ConnectionParameters, MaxConnections int) (p *ConnectionPool, err error) {
	p = new(ConnectionPool)
	p.connectionChannel = make(chan *Connection, MaxConnections)
	p.MaxConnections = MaxConnections

	p.parameters = parameters

	for i := 0; i < p.MaxConnections; i++ {
		var c *Connection
		c, err = Connect(p.parameters)
		if err != nil {
			return
		}
		p.connectionChannel <- c
	}

	return
}

func (p *ConnectionPool) Acquire() (c *Connection) {
	c = <-p.connectionChannel
	return
}

func (p *ConnectionPool) Release(c *Connection) {
	p.connectionChannel <- c
}

func (p *ConnectionPool) Close() {
	for i := 0; i < p.MaxConnections; i++ {
		c := <-p.connectionChannel
		_ = c.Close()
	}
}

// Acquires a connection, delegates the call to that connection, and releases the connection
func (p *ConnectionPool) SelectFunc(sql string, onDataRow func(*DataRowReader) error, arguments ...interface{}) (err error) {
	c := p.Acquire()
	defer p.Release(c)

	return c.SelectFunc(sql, onDataRow, arguments...)
}

// Acquires a connection, delegates the call to that connection, and releases the connection
func (p *ConnectionPool) SelectRows(sql string, arguments ...interface{}) (rows []map[string]interface{}, err error) {
	c := p.Acquire()
	defer p.Release(c)

	return c.SelectRows(sql, arguments...)
}

// Acquires a connection, delegates the call to that connection, and releases the connection
func (p *ConnectionPool) SelectRow(sql string, arguments ...interface{}) (row map[string]interface{}, err error) {
	c := p.Acquire()
	defer p.Release(c)

	return c.SelectRow(sql, arguments...)
}

// Acquires a connection, delegates the call to that connection, and releases the connection
func (p *ConnectionPool) SelectValue(sql string, arguments ...interface{}) (v interface{}, err error) {
	c := p.Acquire()
	defer p.Release(c)

	return c.SelectValue(sql, arguments...)
}

// Acquires a connection, delegates the call to that connection, and releases the connection
func (p *ConnectionPool) SelectValues(sql string, arguments ...interface{}) (values []interface{}, err error) {
	c := p.Acquire()
	defer p.Release(c)

	return c.SelectValues(sql, arguments...)
}
