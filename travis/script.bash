#!/usr/bin/env bash
# source: https://github.com/jackc/pgx/blob/master/travis/script.bash
set -eux

go test -v -race ./...
