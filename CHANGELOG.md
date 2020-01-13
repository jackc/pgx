# 1.2.1 (January 13, 2020)

* Fix data race in context cancellation introduced in v1.2.0.

# 1.2.0 (January 11, 2020)

## Features

* Add Insert(), Update(), Delete(), and Select() statement type query methods to CommandTag.
* Add PgError.SQLState method. This could be used for compatibility with other drivers and databases.

## Performance

* Improve performance when context.Background() is used. (bakape)
* CommandTag.RowsAffected is faster and does not allocate.

## Fixes

* Try to cancel any in-progress query when a conn is closed by ctx cancel.
* Handle NoticeResponse during CopyFrom.
* Ignore errors sending Terminate message while closing connection. This mimics the behavior of libpq PGfinish.

# 1.1.0 (October 12, 2019)

* Add PgConn.IsBusy() method.

# 1.0.1 (September 19, 2019)

* Fix statement cache not properly cleaning discarded statements.
