#!/usr/bin/env bash
set -eux

if [ "${PGVERSION-}" != "" ]
then
  psql -U postgres -c 'create database pgx_test'
  psql -U postgres pgx_test -c 'create domain uint64 as numeric(20,0)'
  psql -U postgres -c "create user pgx_md5 SUPERUSER PASSWORD 'secret'"
fi
