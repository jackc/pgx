# Unreleased v5

## Merged Packages

`github.com/jackc/pgtype`, `github.com/jackc/pgconn`, and `github.com/jackc/pgproto3` are now included in the main `github.com/jackc/pgx` repository. Previously there was confusion as to where issues should be reported, additional release work due to releasing multiple packages, and less clear changelogs.

## pgconn

`CommandTag` is now an opaque type instead of directly exposing an underlying `[]byte`.

## pgtype

The `pgtype` package has been significantly changed.

### NULL Representation

Previously, types had a `Status` field that could be `Undefined`, `Null`, or `Present`. This has been changed to a `Valid` `bool` field to harmonize with how `database/sql` represents NULL and to make the zero value useable.

### Codec and Value Split

Previously, the type system combined decoding and encoding values with the value types. e.g. Type `Int8` both handled encoding and decoding the PostgreSQL representation and acted as a value object. This caused some difficulties when there was not an exact 1 to 1 relationship between the Go types and the PostgreSQL types For example, scanning a PostgreSQL binary `numeric` into a Go `float64` was awkward (see https://github.com/jackc/pgtype/issues/147). This concepts have been separated. A `Codec` only has responsibility for encoding and decoding values. Value types are generally defined by implementing an interface that a particular `Codec` understands (e.g. `PointScanner` and `PointValuer` for the PostgreSQL `point` type).

### Array Types

All array types are now handled by `ArrayCodec` instead of using code generation for each new array type. This significantly reduced the amount of code and the compiled binary size. This also means that less common array types such as `point[]` are now supported.

### Composite Types

Composite types must be registered before use. `CompositeFields` may still be used to construct and destruct composite values, but any type may now implement `CompositeIndexGetter` and `CompositeIndexScanner` to be used as a composite.

### pgxtype

load data type moved to conn

### Bytea

The `Bytea` and `GenericBinary` types have been replaced. Use the following instead:

* `[]byte` - For normal usage directly use `[]byte`.
* `DriverBytes` - Uses driver memory only available until next database method call. Avoids a copy and an allocation.
* `PreallocBytes` - Uses preallocated byte slice to avoid an allocation.
* `UndecodedBytes` - Avoids any decoding. Allows working with raw bytes.

### Dropped lib/pq Support

`pgtype` previously supported and was tested against [lib/pq](https://github.com/lib/pq). While it will continue to work in most cases this is no longer supported.

### database/sql Scan

Previously, most `Scan` implementations would convert `[]byte` to `string` automatically to decode a text value. Now only `string` is handled. This is to allow the possibility of future binary support in `database/sql` mode by considering `[]byte` to be binary format and `string` text format. This change should have no effect for any use with `pgx`. The previous behavior was only necessary for `lib/pq` compatibility.

### Number Type Fields Include Bit size

`Int2`, `Int4`, `Int8`, `Float4`, `Float8`, and `Uint32` fields now include bit size. e.g. `Int` is renamed to `Int64`. This matches the convention set by `database/sql`. In addition, for comparable types like `pgtype.Int8` and `sql.NullInt64` the structures are identical. This means they can be directly converted one to another.

### 3rd Party Type Integrations

* Extracted integrations with github.com/shopspring/decimal and github.com/gofrs/uuid to https://github.com/jackc/pgx-shopspring-decimal and https://github.com/jackc/pgx-gofrs-uuid respectively. This trims the pgx dependency tree.

### Other Changes

* `Bit` and `Varbit` are both replaced by the `Bits` type.
* `CID`, `OID`, `OIDValue`, and `XID` are replaced by the `Uint32` type.
* `Hstore` is now defined as `map[string]*string`.
* `JSON` and `JSONB` types removed. Use `[]byte` or `string` directly.
* `QChar` type removed. Use `rune` or `byte` directly.
* `Macaddr` type removed. Use `net.HardwareAddr` directly.
* Renamed `pgtype.ConnInfo` to `pgtype.Map`.
* Renamed `pgtype.DataType` to `pgtype.Type`.
* Renamed `pgtype.None` to `pgtype.Finite`.
* `RegisterType` now accepts a `*Type` instead of `Type`.

## Reduced Memory Usage by Reusing Read Buffers

Previously, the connection read buffer would allocate large chunks of memory and never reuse them. This allowed transferring ownership to anything such as scanned values without incurring an additional allocation and memory copy. However, this came at the cost of overall increased memory allocation size. But worse it was also possible to pin large chunks of memory by retaining a reference to a small value that originally came directly from the read buffer. Now ownership remains with the read buffer and anything needing to retain a value must make a copy.

## Query Execution Modes

Control over automatic prepared statement caching and simple protocol use are now combined into query execution mode. See documentation for `QueryExecMode`.

## 3rd Party Logger Integration

All integrations with 3rd party loggers have been extracted to separate repositories. This trims the pgx dependency tree.
