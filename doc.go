// Package pgx is a PostgreSQL database driver.
/*
pgx provides lower level access to PostgreSQL than the standard database/sql
It remains as similar to the database/sql interface as possible while
providing better speed and access to PostgreSQL specific features. Import
github.com/jack/pgx/stdlib to use pgx as a database/sql compatible driver.

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
*/
package pgx
