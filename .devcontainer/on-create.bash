#!/bin/bash
set -e

sudo chown vscode:vscode /persist/local /persist/shared
mkdir -p /persist/shared/{claude,atuin/{config,data},go,go-cache,mise/{data,cache},psql,devcontainer-downloads}

mise trust
mise install
