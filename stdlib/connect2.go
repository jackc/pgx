package stdlib

import (
	"context"
	"database/sql"
	"database/sql/driver"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Options2 supplies physical connection inputs and database/sql reuse hooks.
// Connection.AfterConnect runs once per new physical connection. ResetSession
// runs when database/sql reuses that connection.
type Options2 struct {
	Connection   pgx.ConnectOptions2
	ResetSession func(context.Context, *pgx.Conn) error
	ShouldPing   func(context.Context, ShouldPingParams) bool
}

// GetConnector2 captures the original string and Go inputs. Each Connect call
// invokes pgx.Connect2, including after database/sql retires a connection.
func GetConnector2(connString string, opts Options2) driver.Connector {
	connection := opts.Connection.Copy()
	reset := opts.ResetSession
	if reset == nil {
		reset = func(context.Context, *pgx.Conn) error { return nil }
	}
	return connector{connect: func(ctx context.Context) (*pgx.Conn, error) {
		c, err := pgx.Connect2(ctx, connString, connection)
		if err != nil {
			return nil, err
		}
		return c.LegacyConn(), nil
	}, AfterConnect: func(context.Context, *pgx.Conn) error { return nil }, ResetSession: reset, ShouldPing: opts.ShouldPing, driver: pgxDriver}
}

// OpenDB2 opens database/sql with the fresh-input connection strategy. Like
// sql.OpenDB it is lazy; call PingContext to establish and validate a connection.
func OpenDB2(connString string, opts Options2) *sql.DB {
	return sql.OpenDB(GetConnector2(connString, opts))
}

// GetPoolConnector2 acquires and releases through the pool's selected factory.
// Set database/sql's maximum idle connections to zero when using this connector.
func GetPoolConnector2(pool *pgxpool.Pool2) driver.Connector {
	return GetPoolConnector(pool.LegacyPool())
}

// OpenDBFromPool2 shares pool ownership and keeps database/sql's idle count zero.
// Closing the returned DB does not close the pool.
func OpenDBFromPool2(pool *pgxpool.Pool2) *sql.DB {
	db := sql.OpenDB(GetPoolConnector2(pool))
	db.SetMaxIdleConns(0)
	return db
}
