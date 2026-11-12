package pgx

import (
	"context"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// Conn2 is a connection without a parsed-configuration API. Query operations
// share Conn's implementation. Retain original string/options to reconnect;
// PgConn().ConnectionInfo() describes the selected endpoint without credentials.
// Like Conn, Conn2 requires exclusive ownership during use.
type Conn2 struct {
	core *Conn
}

// AsConn2 returns a view sharing c's ownership and lifetime. It does not establish
// or copy a connection. This is useful in existing query tracer and Tx callbacks.
func (c *Conn) AsConn2() *Conn2 { return &Conn2{core: c} }

// LegacyConn is an explicit interoperability bridge for APIs requiring *Conn.
// It shares ownership; callers must not use the two views concurrently. For
// factory-created connections its Config method returns nil. Query tracers and
// Tx.Conn also receive this compatibility view; neither can reconstruct inputs.
func (c *Conn2) LegacyConn() *Conn { return c.core }

// Close delegates to [Conn.Close].
func (c *Conn2) Close(ctx context.Context) error { return c.core.Close(ctx) }

// Prepare delegates to [Conn.Prepare].
func (c *Conn2) Prepare(ctx context.Context, name, sql string) (sd *pgconn.StatementDescription, err error) {
	return c.core.Prepare(ctx, name, sql)
}

// Deallocate delegates to [Conn.Deallocate].
func (c *Conn2) Deallocate(ctx context.Context, name string) error {
	return c.core.Deallocate(ctx, name)
}

// DeallocateAll delegates to [Conn.DeallocateAll].
func (c *Conn2) DeallocateAll(ctx context.Context) error { return c.core.DeallocateAll(ctx) }

// WaitForNotification delegates to [Conn.WaitForNotification].
func (c *Conn2) WaitForNotification(ctx context.Context) (*pgconn.Notification, error) {
	return c.core.WaitForNotification(ctx)
}

// IsClosed delegates to [Conn.IsClosed].
func (c *Conn2) IsClosed() bool { return c.core.IsClosed() }

// Ping delegates to [Conn.Ping].
func (c *Conn2) Ping(ctx context.Context) error { return c.core.Ping(ctx) }

// PgConn delegates to [Conn.PgConn].
func (c *Conn2) PgConn() *pgconn.PgConn { return c.core.PgConn() }

// TypeMap delegates to [Conn.TypeMap].
func (c *Conn2) TypeMap() *pgtype.Map { return c.core.TypeMap() }

// Exec delegates to [Conn.Exec].
func (c *Conn2) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	return c.core.Exec(ctx, sql, arguments...)
}

// Query delegates to [Conn.Query].
func (c *Conn2) Query(ctx context.Context, sql string, args ...any) (Rows, error) {
	return c.core.Query(ctx, sql, args...)
}

// QueryRow delegates to [Conn.QueryRow].
func (c *Conn2) QueryRow(ctx context.Context, sql string, args ...any) Row {
	return c.core.QueryRow(ctx, sql, args...)
}

// SendBatch delegates to [Conn.SendBatch].
func (c *Conn2) SendBatch(ctx context.Context, b *Batch) BatchResults {
	return c.core.SendBatch(ctx, b)
}

// LoadType delegates to [Conn.LoadType].
func (c *Conn2) LoadType(ctx context.Context, typeName string) (*pgtype.Type, error) {
	return c.core.LoadType(ctx, typeName)
}

// LoadTypes delegates to [Conn.LoadTypes].
func (c *Conn2) LoadTypes(ctx context.Context, typeNames []string) ([]*pgtype.Type, error) {
	return c.core.LoadTypes(ctx, typeNames)
}

// CopyFrom delegates to [Conn.CopyFrom].
func (c *Conn2) CopyFrom(ctx context.Context, tableName Identifier, columnNames []string, rowSrc CopyFromSource) (int64, error) {
	return c.core.CopyFrom(ctx, tableName, columnNames, rowSrc)
}

// Begin delegates to [Conn.Begin].
func (c *Conn2) Begin(ctx context.Context) (Tx, error) { return c.core.Begin(ctx) }

// BeginTx delegates to [Conn.BeginTx].
func (c *Conn2) BeginTx(ctx context.Context, txOptions TxOptions) (Tx, error) {
	return c.core.BeginTx(ctx, txOptions)
}
