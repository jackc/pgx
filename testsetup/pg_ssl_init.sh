#!/bin/bash
# Docker initdb script: copies SSL certificates to PGDATA with correct
# permissions and enables SSL. Runs as the postgres user during container
# initialization.
base64 -d /etc/postgresql/ssl/localhost.crt.b64 > "$PGDATA/server.crt"
base64 -d /etc/postgresql/ssl/localhost.key.b64 > "$PGDATA/server.key"
base64 -d /etc/postgresql/ssl/ca.pem.b64 > "$PGDATA/root.crt"
chmod 600 "$PGDATA/server.key"

# Append SSL config to postgresql.conf rather than using command-line flags,
# because the docker entrypoint passes command-line args to the temporary server
# it starts before initdb scripts run. That temp server would fail with ssl=on
# since the cert files don't exist yet.
cat /etc/postgresql/postgresql_ssl.conf >> "$PGDATA/postgresql.conf"
