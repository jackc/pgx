#!/usr/bin/env bash
# source: https://github.com/jackc/pgx/blob/master/travis/script.bash
set -eux

if [ "${PGVERSION-}" != "" ]
then
  go test -v -race ./...
elif [ "${CRATEVERSION-}" != "" ]
then
  go test -v -race -run 'TestCrateDBConnect'
fi
