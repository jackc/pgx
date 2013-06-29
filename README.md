pgx
===

Experimental PostgreSQL client library for Go

Usage
=====

TODO

Development
===========

Testing
-------

To setup the test environment run the test_setup.sql script as a user that can
create users and databases. To successfully run the connection tests for various
means of authentication you must include the following in your pg_hba.conf.

    local  pgx_test  pgx_none  trust
    local  pgx_test  pgx_pw    password
    local  pgx_test  pgx_md5   md5


