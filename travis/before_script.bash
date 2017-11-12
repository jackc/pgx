#!/usr/bin/env bash
set -eux

mv conn_config_test.go.travis conn_config_test.go
psql -U postgres -c 'create database pgx_test'
psql -U postgres pgx_test -c 'create extension hstore'
psql -U postgres -c "create user pgx_ssl SUPERUSER PASSWORD 'secret'"
psql -U postgres -c "create user pgx_md5 SUPERUSER PASSWORD 'secret'"
psql -U postgres -c "create user pgx_pw  SUPERUSER PASSWORD 'secret'"
psql -U postgres -c "create user pgx_replication with replication password 'secret'"
psql -U postgres -c "create user \" tricky, ' } \"\" \\ test user \" superuser password 'secret'"
