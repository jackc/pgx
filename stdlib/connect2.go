package stdlib

import (
	"context"
	"database/sql"
	"database/sql/driver"

	"github.com/jackc/pgx/v5"
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
		return pgx.Connect2(ctx, connString, connection)
	}, AfterConnect: func(context.Context, *pgx.Conn) error { return nil }, ResetSession: reset, ShouldPing: opts.ShouldPing, driver: pgxDriver}
}

// OpenDB2 opens database/sql with the fresh-input connection strategy. Like
// sql.OpenDB it is lazy; call PingContext to establish and validate a connection.
func OpenDB2(connString string, opts Options2) *sql.DB {
	return sql.OpenDB(GetConnector2(connString, opts))
}
