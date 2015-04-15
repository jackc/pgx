# 2.5.0 (April 15, 2015)

* Fix stdlib nil support (Blaž Hrastnik)
* Support custom Scanner not reading entire value
* Fix empty array scanning (Laurent Debacker)
* Add ParseDSN (deoxxa)
* Add timestamp support to NullTime
* Remove unused text format scanners
* Return error when too many parameters on Prepare
* Add Travis CI integration (Jonathan Rudenberg)
* Large object support (Blake Gentry)
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
