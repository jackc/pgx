[![](https://godoc.org/github.com/jackc/pgpassfile?status.svg)](https://godoc.org/github.com/jackc/pgpassfile)
[![Build Status](https://travis-ci.org/jackc/pgpassfile.svg)](https://travis-ci.org/jackc/pgpassfile)

# pgio

Package pgio is a low-level toolkit building messages in the PostgreSQL wire protocol.

pgio provides functions for appending integers to a []byte while doing byte
order conversion.

Extracted from original implementation in https://github.com/jackc/pgx.
