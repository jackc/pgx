# pgx

PostgreSQL client library for Go

## Description

pgx is a database connection library designed specifically for PostgreSQL. pgx offers an interface similar to database/sql that offers more performance and features than are available the database/sql interface. It also can run as a database/sql compatible driver by importing github.com/jackc/pgx/stdlib.

## Features

Below are some of the standout features of pgx.

### Familiar Query Interface

pgx implements Query, QueryRow, and Scan in the familiar database/sql style.

```go
var name string
var weight int64
err := conn.QueryRow("select name, weight from widgets where id=$1", 42).Scan(&name, &weight)
if err != nil {
    return err
}
```

pgx adds convenience to Query in that it is only necessary to call Close if you
want to ignore the rest of the rows. When Next has read all rows or an error
occurs, the rows are closed automatically.

```go
var sum int32

rows, err := conn.Query("select generate_series(1,$1)", 10)
if err != nil {
    t.Fatalf("conn.Query failed: ", err)
}

for rows.Next() {
    var n int32
    rows.Scan(&n)
    sum += n
}

// rows.Close implicitly called when rows.Next is finished

if rows.Err() != nil {
    t.Fatalf("conn.Query failed: ", err)
}

// ...
```

### Prepared Statements

Prepared statements are easy to use in pgx. Just call Prepare with the name of
the statement and the SQL. To execute a prepared statement just pass the name
of the statement into a Query, QueryRow, or Exec as the SQL text. It will
automatically detect that it is the name of a prepared statement and execute
it.

```go
if _, err := conn.Prepare("getTime", "select now()"); err == nil {
    // handle err
}

var t time.Time
err := conn.QueryRow("getTime").Scan(&t)
if err != nil {
    return err
}
```

Prepared statements will use the binary transmission when possible. This can
substantially increase performance.

### Explicit Connection Pool

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

```go
var name string
var weight int64
err := pool.QueryRow("select name, weight from widgets where id=$1", 42).Scan(&name, &weight)
if err != nil {
    return err
}
```

### Transactions

Transactions are started by calling Begin or BeginIso. The BeginIso variant
creates a transaction with a specified isolation level.

```go
    tx, err := conn.Begin()
    if err != nil {
        t.Fatalf("conn.Begin failed: %v", err)
    }

    _, err = tx.Exec("insert into foo(id) values (1)")
    if err != nil {
        t.Fatalf("tx.Exec failed: %v", err)
    }

    err = tx.Commit()
    if err != nil {
        t.Fatalf("tx.Commit failed: %v", err)
    }
})
```

### Listen / Notify

Pgx can listen to the PostgreSQL notification system with the
WaitForNotification function. It takes a maximum time to wait for a
notification.

```go
if notification, err := conn.WaitForNotification(time.Second); err != nil {
    // do something with notification
}
```

### TLS

The pgx ConnConfig struct has a TLSConfig field. If this field is
nil, then TLS will be disabled. If it is present, then it will be used to
configure the TLS connection.

### Custom Type Support

pgx includes support for the common data types like integers, floats, strings,
dates, and times that have direct mappings between Go and SQL. Support can be
added for additional types like point, hstore, numeric, etc. that do not have
direct mappings in Go by the types implementing Scanner, TextEncoder, and
optionally BinaryEncoder. To enable binary format for custom types, a prepared
statement must be used and the field description of the returned field must have
FormatCode set to BinaryFormatCode. See example_custom_type_test.go for an
example of a custom type for the PostgreSQL point type.

### Null Mapping

pgx includes Null* types in a similar fashion to database/sql that implement the
necessary interfaces to be encoded and scanned.

### Logging

pgx connections optionally accept a logger from the [log15 package](http://gopkg.in/inconshreveable/log15.v2).

## Testing

Pgx supports multiple connection and authentication types. Setting up a test
environment that can test all of them can be cumbersome. In particular,
Windows cannot test Unix domain socket connections. Because of this pgx will
skip tests for connection types that are not configured.

### Normal Test Environment

To setup the normal test environment run the following SQL:

    create user pgx_md5 password 'secret';
    create database pgx_test;

Next open connection_settings_test.go.example and make a copy without the
.example. If your PostgreSQL server is accepting connections on 127.0.0.1,
then you are done.

### Connection and Authentication Test Environment

Complete the normal test environment setup and also do the following.

Run the following SQL:

    create user pgx_none;
    create user pgx_pw password 'secret';

Add the following to your pg_hba.conf:

If you are developing on Unix with domain socket connections:

    local  pgx_test  pgx_none  trust
    local  pgx_test  pgx_pw    password
    local  pgx_test  pgx_md5   md5

If you are developing on Windows with TCP connections:

    host  pgx_test  pgx_none  127.0.0.1/32 trust
    host  pgx_test  pgx_pw    127.0.0.1/32 password
    host  pgx_test  pgx_md5   127.0.0.1/32 md5
