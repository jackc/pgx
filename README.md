# pgx

PostgreSQL client library for Go

## Description

Pgx is a database connection library designed specifically for PostgreSQL.
There are features of PostgreSQL that are difficult or impossible to use with
the standard Go library SQL interface. This library trades conformity with the
standard interface for ease of use and the power that is available when
working directly with PostgreSQL.

## Features

Below are some of the standout features of pgx.

### Simple Query Interface

Pgx has easy to use functions for common query operations like SelectValue,
SelectValues, SelectRow, and SelectRows. These can be easier to use than the
standard Scan interface. These directly return interface{}, []interface{},
map[string]interface{}, and []map[string]interface{} respectively. SelectFunc
offers custom row by row processing.

```go
if widgets, err := conn.SelectRows("select name, weight from widgets where type=$1", type); err != nil {
    for w := range widgets {
        fmt.Printf("%v has a weight of %v.", widgets["name"], widgets["weight"])
    }
}
```

### Prepared Statements

Prepared statements are easy to use in pgx. Just call Prepare with the name of
the statement and the SQL. To execute a prepared statement just pass the name
of the statement into a Select* or Execute command as the SQL text. It will
automatically detect that it is the name of a prepared statement and execute
it.

```go
if err := conn.Prepare("getTime", "select now()"); err == nil {
    // handle err
}
if time, err := conn.SelectValue("getTime"); err != nil {
    // do something with time
}
```

Prepared statements will use the binary transmission format for types that
have a binary transcoder available (this can substantially reduce overhead
when using the bytea type).

### Explicit Connection Pool

Connection pool usage is explicit and configurable. In pgx, a connection can
be created and managed directly, or a connection pool with a configurable
maximum connections can be used. Also, the connection pool offers an after
connect hook that allows every connection to be automatically setup before
being made available in the connection pool. This is especially useful to
ensure all connections have the same prepared statements available or to
change any other connection settings.

It also delegates Select* and Execute functions to an automatically checked
out and released connection so you can avoid manually acquiring and releasing
connections when you do not need that level of control.

```go
if widgets, err := pool.SelectRows("select * from widgets where type=$1", type); err != nil {
    // do something with widgets
}
```

### Transactions

Transactions are are used by passing a function to the Transaction function.
This function ensures that the transaction is committed or rolled back
automatically. The TransactionIso variant creates a transaction with a
specified isolation level.

```go
committed, err := conn.TransactionIso("serializable", func() bool {
    // do something with transaction
    return true // return true to commit / false to rollback
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

The pgx ConnectionParameters struct has a TLSConfig field. If this field is
nil, then TLS will be disabled. If it is present, then it will be used to
configure the TLS connection.

### Custom Transcoder Support

Pgx includes transcoders for the common data types like integers, floats,
strings, dates, and times that have direct mappings between Go and SQL.
Transcoders can be added for additional types like point, hstore, numeric,
etc. that do not have direct mappings in Go. pgx.ValueTranscoders is a map of
PostgreSQL OID's to transcoders. All that is needed to add or change how a
data type is to set that OID's transcoder. See
example_value_transcoder_test.go for an example of a custom transcoder for the
PostgreSQL point type.

### SelectValueTo

There are some cases where Go is used as an HTTP server that is directly
relaying single values from PostgreSQL (such as JSON or binary blobs).
SelectValueTo copies the single returned value directly from PostgreSQL to a
io.Writer. This can be faster than SelectValue then write especially when the
values are at least many KB in size.

### Null Mapping

As pgx uses interface{} for all values SQL nulls are mapped to nil. This
eliminates the need for wrapping values in structs that include a boolean for
the null possibility. On the other hand, returned values usually must be type
asserted before use. It also presents difficulties dealing with complex types
such as arrays. pgx directly maps a Go []int32 to a PostgreSQL int4[]. The
problem is the PostgreSQL array can include nulls, but the Go slice cannot.
Array transcoding should be considered experimental. On the plus side, because
of the pluggable transcoder support, an application that wished to handle
arrays (or any types) differently can easily override the default transcoding
(so even using a strict with value and null fields would simply be a matter of
changing transcoders).

### Logging

Pgx defines the pgx.Logger interface. A value that satisfies this interface
used as part of ConnectionOptions or ConnectionPoolOptions to enable logging
of pgx activities.

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
