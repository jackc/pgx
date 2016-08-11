// Package pgx is a PostgreSQL database driver.
/*
pgx provides lower level access to PostgreSQL than the standard database/sql
It remains as similar to the database/sql interface as possible while
providing better speed and access to PostgreSQL specific features. Import
github.com/jack/pgx/stdlib to use pgx as a database/sql compatible driver.

Query Interface

pgx implements Query and Scan in the familiar database/sql style.

    var sum int32

    // Send the query to the server. The returned rows MUST be closed
    // before conn can be used again.
    rows, err := conn.Query("select generate_series(1,$1)", 10)
    if err != nil {
        return err
    }

    // rows.Close is called by rows.Next when all rows are read
    // or an error occurs in Next or Scan. So it may optionally be
    // omitted if nothing in the rows.Next loop can panic. It is
    // safe to close rows multiple times.
    defer rows.Close()

    // Iterate through the result set
    for rows.Next() {
        var n int32
        err = rows.Scan(&n)
        if err != nil {
            return err
        }
        sum += n
    }

    // Any errors encountered by rows.Next or rows.Scan will be returned here
    if rows.Err() != nil {
        return err
    }

    // No errors found - do something with sum

pgx also implements QueryRow in the same style as database/sql.

    var name string
    var weight int64
    err := conn.QueryRow("select name, weight from widgets where id=$1", 42).Scan(&name, &weight)
    if err != nil {
        return err
    }

Use Exec to execute a query that does not return a result set.

    commandTag, err := conn.Exec("delete from widgets where id=$1", 42)
    if err != nil {
        return err
    }
    if commandTag.RowsAffected() != 1 {
        return errors.New("No row found to delete")
    }

Connection Pool

Connection pool usage is explicit and configurable. In pgx, a connection can
be created and managed directly, or a connection pool with a configurable
maximum connections can be used. Also, the connection pool offers an after
connect hook that allows every connection to be automatically setup before
being made available in the connection pool. This is especially useful to
ensure all connections have the same prepared statements available or to
change any other connection settings.

It delegates Query, QueryRow, Exec, and Begin functions to an automatically
checked out and released connection so you can avoid manually acquiring and
releasing connections when you do not need that level of control.

    var name string
    var weight int64
    err := pool.QueryRow("select name, weight from widgets where id=$1", 42).Scan(&name, &weight)
    if err != nil {
        return err
    }

Transactions

Transactions are started by calling Begin or BeginIso. The BeginIso variant
creates a transaction with a specified isolation level.

    tx, err := conn.Begin()
    if err != nil {
        return err
    }
    // Rollback is safe to call even if the tx is already closed, so if
    // the tx commits successfully, this is a no-op
    defer tx.Rollback()

    _, err = tx.Exec("insert into foo(id) values (1)")
    if err != nil {
        return err
    }

    err = tx.Commit()
    if err != nil {
        return err
    }

Copy Protocol

Use CopyTo to efficiently insert multiple rows at a time using the PostgreSQL
copy protocol. CopyTo accepts a CopyToSource interface. If the data is already
in a [][]interface{} use CopyToRows to wrap it in a CopyToSource interface. Or
implement CopyToSource to avoid buffering the entire data set in memory.

    rows := [][]interface{}{
        {"John", "Smith", int32(36)},
        {"Jane", "Doe", int32(29)},
    }

    copyCount, err := conn.CopyTo(
        "people",
        []string{"first_name", "last_name", "age"},
        pgx.CopyToRows(rows),
    )

CopyTo can be faster than an insert with as few as 5 rows.

Listen and Notify

pgx can listen to the PostgreSQL notification system with the
WaitForNotification function. It takes a maximum time to wait for a
notification.

    err := conn.Listen("channelname")
    if err != nil {
        return nil
    }

    if notification, err := conn.WaitForNotification(time.Second); err != nil {
        // do something with notification
    }

Null Mapping

pgx can map nulls in two ways. The first is Null* types that have a data field
and a valid field. They work in a similar fashion to database/sql. The second
is to use a pointer to a pointer.

    var foo pgx.NullString
    var bar *string
    err := conn.QueryRow("select foo, bar from widgets where id=$1", 42).Scan(&a, &b)
    if err != nil {
        return err
    }

Array Mapping

pgx maps between int16, int32, int64, float32, float64, and string Go slices
and the equivalent PostgreSQL array type. Go slices of native types do not
support nulls, so if a PostgreSQL array that contains a null is read into a
native Go slice an error will occur.

Hstore Mapping

pgx includes an Hstore type and a NullHstore type. Hstore is simply a
map[string]string and is preferred when the hstore contains no nulls. NullHstore
follows the Null* pattern and supports null values.

JSON and JSONB Mapping

pgx includes built-in support to marshal and unmarshal between Go types and
the PostgreSQL JSON and JSONB.

Inet and Cidr Mapping

pgx encodes from net.IPNet to and from inet and cidr PostgreSQL types. In
addition, as a convenience pgx will encode from a net.IP; it will assume a /32
netmask for IPv4 and a /128 for IPv6.

Custom Type Support

pgx includes support for the common data types like integers, floats, strings,
dates, and times that have direct mappings between Go and SQL. Support can be
added for additional types like point, hstore, numeric, etc. that do not have
direct mappings in Go by the types implementing Scanner and Encoder.

Custom types can support text or binary formats. Binary format can provide a
large performance increase. The natural place for deciding the format for a
value would be in Scanner as it is responsible for decoding the returned data.
However, that is impossible as the query has already been sent by the time the
Scanner is invoked. The solution to this is the global DefaultTypeFormats. If a
custom type prefers binary format it should register it there.

        pgx.DefaultTypeFormats["point"] = pgx.BinaryFormatCode

Note that the type is referred to by name, not by OID. This is because custom
PostgreSQL types like hstore will have different OIDs on different servers. When
pgx establishes a connection it queries the pg_type table for all types. It then
matches the names in DefaultTypeFormats with the returned OIDs and stores it in
Conn.PgTypes.

See example_custom_type_test.go for an example of a custom type for the
PostgreSQL point type.

pgx also includes support for custom types implementing the database/sql.Scanner
and database/sql/driver.Valuer interfaces.

Raw Bytes Mapping

[]byte passed as arguments to Query, QueryRow, and Exec are passed unmodified
to PostgreSQL. In like manner, a *[]byte passed to Scan will be filled with
the raw bytes returned by PostgreSQL. This can be especially useful for reading
varchar, text, json, and jsonb values directly into a []byte and avoiding the
type conversion from string.

TLS

The pgx ConnConfig struct has a TLSConfig field. If this field is
nil, then TLS will be disabled. If it is present, then it will be used to
configure the TLS connection. This allows total configuration of the TLS
connection.

Logging

pgx defines a simple logger interface. Connections optionally accept a logger
that satisfies this interface. The log15 package
(http://gopkg.in/inconshreveable/log15.v2) satisfies this interface and it is
simple to define adapters for other loggers. Set LogLevel to control logging
verbosity.
*/
package pgx
