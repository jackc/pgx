#!/bin/bash
# Docker initdb script: copies SSL certificates to PGDATA with correct permissions.
# Runs as the postgres user during container initialization.
base64 -d /etc/postgresql/ssl/localhost.crt.b64 > "$PGDATA/server.crt"
base64 -d /etc/postgresql/ssl/localhost.key.b64 > "$PGDATA/server.key"
base64 -d /etc/postgresql/ssl/ca.pem.b64 > "$PGDATA/root.crt"
chmod 600 "$PGDATA/server.key"
