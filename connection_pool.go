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
