# 5.0.4 (October 24, 2022)

* Fix: CollectOneRow prefers PostgreSQL error over pgx.ErrorNoRows
* Fix: some reflect Kind checks to first check for nil
* Bump golang.org/x/text dependency to placate snyk
* Fix: RowToStructByPos on structs with multiple anonymous sub-structs (Baptiste Fontaine)
* Fix: Exec checks if tx is closed

# 5.0.3 (October 14, 2022)

* Fix `driver.Valuer` handling edge cases that could cause infinite loop or crash

# v5.0.2 (October 8, 2022)

* Fix date encoding in text format to always use 2 digits for month and day
* Prefer driver.Valuer over wrap plans when encoding
* Fix scan to pointer to pointer to renamed type
* Allow scanning NULL even if PG and Go types are incompatible

# v5.0.1 (September 24, 2022)

* Fix 32-bit atomic usage
* Add MarshalJSON for Float8 (yogipristiawan)
* Add `[` and `]` to text encoding of `Lseg`
* Fix sqlScannerWrapper NULL handling

# v5.0.0 (September 17, 2022)

## Merged Packages

`github.com/jackc/pgtype`, `github.com/jackc/pgconn`, and `github.com/jackc/pgproto3` are now included in the main
`github.com/jackc/pgx` repository. Previously there was confusion as to where issues should be reported, additional
release work due to releasing multiple packages, and less clear changelogs.

## pgconn

`CommandTag` is now an opaque type instead of directly exposing an underlying `[]byte`.

The return value `ResultReader.Values()` is no longer safe to retain a reference to after a subsequent call to `NextRow()` or `Close()`.

`Trace()` method adds low level message tracing similar to the `PQtrace` function in `libpq`.

pgconn now uses non-blocking IO. This is a significant internal restructuring, but it should not cause any visible changes on its own. However, it is important in implementing other new features.

`CheckConn()` checks a connection's liveness by doing a non-blocking read. This can be used to detect database restarts or network interruptions without executing a query or a ping.

pgconn now supports pipeline mode.

`*PgConn.ReceiveResults` removed. Use pipeline mode instead.

`Timeout()` no longer considers `context.Canceled` as a timeout error. `context.DeadlineExceeded` still is considered a timeout error.

## pgxpool

`Connect` and `ConnectConfig` have been renamed to `New` and `NewWithConfig` respectively. The `LazyConnect` option has been removed. Pools always lazily connect.

## pgtype

The `pgtype` package has been significantly changed.

### NULL Representation

Previously, types had a `Status` field that could be `Undefined`, `Null`, or `Present`. This has been changed to a
`Valid` `bool` field to harmonize with how `database/sql` represents `NULL` and to make the zero value useable.

### Codec and Value Split

Previously, the type system combined decoding and encoding values with the value types. e.g. Type `Int8` both handled
encoding and decoding the PostgreSQL representation and acted as a value object. This caused some difficulties when
there was not an exact 1 to 1 relationship between the Go types and the PostgreSQL types For example, scanning a
PostgreSQL binary `numeric` into a Go `float64` was awkward (see https://github.com/jackc/pgtype/issues/147). This
concepts have been separated. A `Codec` only has responsibility for encoding and decoding values. Value types are
generally defined by implementing an interface that a particular `Codec` understands (e.g. `PointScanner` and
`PointValuer` for the PostgreSQL `point` type).

### Array Types

All array types are now handled by `ArrayCodec` instead of using code generation for each new array type. This also
means that less common array types such as `point[]` are now supported. `Array[T]` supports PostgreSQL multi-dimensional
arrays.

### Composite Types

Composite types must be registered before use. `CompositeFields` may still be used to construct and destruct composite
values, but any type may now implement `CompositeIndexGetter` and `CompositeIndexScanner` to be used as a composite.

### Range Types

Range types are now handled with types `RangeCodec` and `Range[T]`. This allows additional user defined range types to
easily be handled. Multirange types are handled similarly with `MultirangeCodec` and `Multirange[T]`.

### pgxtype

`LoadDataType` moved to `*Conn` as `LoadType`.

### Bytea

The `Bytea` and `GenericBinary` types have been replaced. Use the following instead:

* `[]byte` - For normal usage directly use `[]byte`.
* `DriverBytes` - Uses driver memory only available until next database method call. Avoids a copy and an allocation.
* `PreallocBytes` - Uses preallocated byte slice to avoid an allocation.
* `UndecodedBytes` - Avoids any decoding. Allows working with raw bytes.

### Dropped lib/pq Support

`pgtype` previously supported and was tested against [lib/pq](https://github.com/lib/pq). While it will continue to work
in most cases this is no longer supported.

### database/sql Scan

Previously, most `Scan` implementations would convert `[]byte` to `string` automatically to decode a text value. Now
only `string` is handled. This is to allow the possibility of future binary support in `database/sql` mode by
considering `[]byte` to be binary format and `string` text format. This change should have no effect for any use with
`pgx`. The previous behavior was only necessary for `lib/pq` compatibility.

Added `*Map.SQLScanner` to create a `sql.Scanner` for types such as `[]int32` and `Range[T]` that do not implement
`sql.Scanner` directly.

### Number Type Fields Include Bit size

`Int2`, `Int4`, `Int8`, `Float4`, `Float8`, and `Uint32` fields now include bit size. e.g. `Int` is renamed to `Int64`.
This matches the convention set by `database/sql`. In addition, for comparable types like `pgtype.Int8` and
`sql.NullInt64` the structures are identical. This means they can be directly converted one to another.

### 3rd Party Type Integrations

* Extracted integrations with https://github.com/shopspring/decimal and https://github.com/gofrs/uuid to
  https://github.com/jackc/pgx-shopspring-decimal and https://github.com/jackc/pgx-gofrs-uuid respectively. This trims
  the pgx dependency tree.

### Other Changes

* `Bit` and `Varbit` are both replaced by the `Bits` type.
* `CID`, `OID`, `OIDValue`, and `XID` are replaced by the `Uint32` type.
* `Hstore` is now defined as `map[string]*string`.
* `JSON` and `JSONB` types removed. Use `[]byte` or `string` directly.
* `QChar` type removed. Use `rune` or `byte` directly.
* `Inet` and `Cidr` types removed. Use `netip.Addr` and `netip.Prefix` directly. These types are more memory efficient than the previous `net.IPNet`.
* `Macaddr` type removed. Use `net.HardwareAddr` directly.
* Renamed `pgtype.ConnInfo` to `pgtype.Map`.
* Renamed `pgtype.DataType` to `pgtype.Type`.
* Renamed `pgtype.None` to `pgtype.Finite`.
* `RegisterType` now accepts a `*Type` instead of `Type`.
* Assorted array helper methods and types made private.

## stdlib

* Removed `AcquireConn` and `ReleaseConn` as that functionality has been built in since Go 1.13.

## Reduced Memory Usage by Reusing Read Buffers

Previously, the connection read buffer would allocate large chunks of memory and never reuse them. This allowed
transferring ownership to anything such as scanned values without incurring an additional allocation and memory copy.
However, this came at the cost of overall increased memory allocation size. But worse it was also possible to pin large
chunks of memory by retaining a reference to a small value that originally came directly from the read buffer. Now
ownership remains with the read buffer and anything needing to retain a value must make a copy.

## Query Execution Modes

Control over automatic prepared statement caching and simple protocol use are now combined into query execution mode.
See documentation for `QueryExecMode`.

## QueryRewriter Interface and NamedArgs

pgx now supports named arguments with the `NamedArgs` type. This is implemented via the new `QueryRewriter` interface which
allows arbitrary rewriting of query SQL and arguments.

## RowScanner Interface

The `RowScanner` interface allows a single argument to Rows.Scan to scan the entire row.

## Rows Result Helpers

* `CollectRows` and `RowTo*` functions simplify collecting results into a slice.
* `CollectOneRow` collects one row using `RowTo*` functions.
* `ForEachRow` simplifies scanning each row and executing code using the scanned values. `ForEachRow` replaces `QueryFunc`.

## Tx Helpers

Rather than every type that implemented `Begin` or `BeginTx` methods also needing to implement `BeginFunc` and
`BeginTxFunc` these methods have been converted to functions that take a db that implements `Begin` or `BeginTx`.

## Improved Batch Query Ergonomics

Previously, the code for building a batch went in one place before the call to `SendBatch`, and the code for reading the
results went in one place after the call to `SendBatch`. This could make it difficult to match up the query and the code
to handle the results. Now `Queue` returns a `QueuedQuery` which has methods `Query`, `QueryRow`, and `Exec` which can
be used to register a callback function that will handle the result. Callback functions are called automatically when
`BatchResults.Close` is called.

## SendBatch Uses Pipeline Mode When Appropriate

Previously, a batch with 10 unique parameterized statements executed 100 times would entail 11 network round trips. 1
for each prepare / describe and 1 for executing them all. Now pipeline mode is used to prepare / describe all statements
in a single network round trip. So it would only take 2 round trips.

## Tracing and Logging

Internal logging support has been replaced with tracing hooks. This allows custom tracing integration with tools like OpenTelemetry. Package tracelog provides an adapter for pgx v4 loggers to act as a tracer.

All integrations with 3rd party loggers have been extracted to separate repositories. This trims the pgx dependency
tree.
