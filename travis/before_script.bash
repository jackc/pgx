#!/usr/bin/env bash
# source: https://github.com/jackc/pgx/blob/master/travis/before_script.bash
set -eux

if [ "${PGVERSION-}" != "" ]
then
  psql -U postgres -c 'create database pgx_test'
  psql -U postgres pgx_test -c 'create domain uint64 as numeric(20,0)'
  psql -U postgres -c "create user pgx_md5 SUPERUSER PASSWORD 'secret'"
  psql -U postgres pgx_test -c 'create extension if not exists hstore;'
fi
