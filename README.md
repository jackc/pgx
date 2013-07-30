# pgx

PostgreSQL client library for Go

## Usage

TODO

## Development

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

Comple the normal test environment setup and also do the following.

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
