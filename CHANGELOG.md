# 1.2.0 (February 5, 2020)

* Add zeronull package for easier NULL <-> zero conversion
* Add JSON marshalling for shopspring-numeric extension
* Add JSON marshalling for Bool, Date, JSON/B, Timestamptz (Jeffrey Stiles)
* Fix null status in UnmarshalJSON for some types (Jeffrey Stiles)

# 1.1.0 (January 11, 2020)

* Add PostgreSQL time type support
* Add more automatic conversions of integer arrays of different types (Jean-Philippe Quéméner)

# 1.0.3 (November 16, 2019)

* Support initializing Array types from a slice of the value (Alex Gaynor)

# 1.0.2 (October 22, 2019)

* Fix scan into null into pointer to pointer implementing Decode* interface. (Jeremy Altavilla)

# 1.0.1 (September 19, 2019)

* Fix daterange OID
