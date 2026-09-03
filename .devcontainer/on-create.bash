#!/bin/bash
set -e

sudo chown vscode:vscode /persist/local /persist/shared
mkdir -p /persist/shared/{claude,atuin/{config,data},go,go-cache,mise/{data,cache},psql,devcontainer-downloads}

# PGX_DEV_RUNTIME_DIR (devcontainer.json): this checkout's clusters and their socket directory, on
# the named volume rather than the bind mount. initdb refuses a data directory it does not own, and
# PostgreSQL refuses a socket directory that is group- or world-writable.
mkdir -p /persist/local/dev
chmod 700 /persist/local/dev

mise trust
mise install
