# 2.9.0 (August 26, 2016)

## Fixes

* Fix *ConnPool.Deallocate() not deleting prepared statement from map
* Fix stdlib not logging unprepared query SQL (Krzysztof Dryś)
* Fix Rows.Values() with varchar binary format
* Concurrent ConnPool.Acquire calls with Dialer timeouts now timeout in the expected amount of time (Konstantin Dzreev)

## Features

* Add CopyTo
* Add PrepareEx
* Add basic record to []interface{} decoding
* Encode and decode between all Go and PostgreSQL integer types with bounds checking
* Decode inet/cidr to net.IP
* Encode/decode [][]byte to/from bytea[]
* Encode/decode named types whose underlying types are string, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64

## Performance

* Substantial reduction in memory allocations

# 2.8.1 (March 24, 2016)

## Features

* Scan accepts nil argument to ignore a column

## Fixes

* Fix compilation on 32-bit architecture
* Fix Tx.status not being set on error on Commit
* Fix Listen/Unlisten with special characters

# 2.8.0 (March 18, 2016)

## Fixes

* Fix unrecognized commit failure
* Fix msgReader.rxMsg bug when msgReader already has error
* Go float64 can no longer be encoded to a PostgreSQL float4
* Fix connection corruption when query with error is closed early

## Features

This release adds multiple extension points helpful when wrapping pgx with
custom application behavior. pgx can now use custom types designed for the
standard database/sql package such as
[github.com/shopspring/decimal](https://github.com/shopspring/decimal).

* Add *Tx.AfterClose() hook
* Add *Tx.Conn()
* Add *Tx.Status()
* Add *Tx.Err()
* Add *Rows.AfterClose() hook
* Add *Rows.Conn()
* Add *Conn.SetLogger() to allow changing logger
* Add *Conn.SetLogLevel() to allow changing log level
* Add ConnPool.Reset method
* Add support for database/sql.Scanner and database/sql/driver.Valuer interfaces
* Rows.Scan errors now include which argument caused error
* Add Encode() to allow custom Encoders to reuse internal encoding functionality
* Add Decode() to allow customer Decoders to reuse internal decoding functionality
* Add ConnPool.Prepare method
* Add ConnPool.Deallocate method
* Add Scan to uint32 and uint64 (utrack)
* Add encode and decode to []uint16, []uint32, and []uint64 (Max Musatov)

## Performance

* []byte skips encoding/decoding

# 2.7.1 (October 26, 2015)

* Disable SSL renegotiation

# 2.7.0 (October 16, 2015)

* Add RuntimeParams to ConnConfig
* ParseURI extracts RuntimeParams
* ParseDSN extracts RuntimeParams
* ParseEnvLibpq extracts PGAPPNAME
* Prepare is now idempotent
* Rows.Values now supports oid type
* ConnPool.Release automatically unlistens connections (Joseph Glanville)
* Add trace log level
* Add more efficient log leveling
* Retry automatically on ConnPool.Begin (Joseph Glanville)
* Encode from net.IP to inet and cidr
* Generalize encoding pointer to string to any PostgreSQL type
* Add UUID encoding from pointer to string (Joseph Glanville)
* Add null mapping to pointer to pointer (Jonathan Rudenberg)
* Add JSON and JSONB type support (Joseph Glanville)

# 2.6.0 (September 3, 2015)

* Add inet and cidr type support
* Add binary decoding to TimestampOid in stdlib driver (Samuel Stauffer)
* Add support for specifying sslmode in connection strings (Rick Snyder)
* Allow ConnPool to have MaxConnections of 1
* Add basic PGSSLMODE to support to ParseEnvLibpq
* Add fallback TLS config
* Expose specific error for TSL refused
* More error details exposed in PgError
* Support custom dialer (Lewis Marshall)

# 2.5.0 (April 15, 2015)

* Fix stdlib nil support (Blaž Hrastnik)
* Support custom Scanner not reading entire value
* Fix empty array scanning (Laurent Debacker)
* Add ParseDSN (deoxxa)
* Add timestamp support to NullTime
* Remove unused text format scanners
* Return error when too many parameters on Prepare
* Add Travis CI integration (Jonathan Rudenberg)
* Large object support (Jonathan Rudenberg)
* Fix reading null byte arrays (Karl Seguin)
* Add timestamptz[] support
* Add timestamp[] support (Karl Seguin)
* Add bool[] support (Karl Seguin)
* Allow writing []byte into text and varchar columns without type conversion (Hari Bhaskaran)
* Fix ConnPool Close panic
* Add Listen / notify example
* Reduce memory allocations (Karl Seguin)

# 2.4.0 (October 3, 2014)

* Add per connection oid to name map
* Add Hstore support (Andy Walker)
* Move introductory docs to godoc from readme
* Fix documentation references to TextEncoder and BinaryEncoder
* Add keep-alive to TCP connections (Andy Walker)
* Add support for EmptyQueryResponse / Allow no-op Exec (Andy Walker)
* Allow reading any type into []byte
* WaitForNotification detects lost connections quicker

# 2.3.0 (September 16, 2014)

* Truncate logged strings and byte slices
* Extract more error information from PostgreSQL
* Fix data race with Rows and ConnPool
