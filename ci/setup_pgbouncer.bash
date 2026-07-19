#!/usr/bin/env bash
set -euxo pipefail

sudo apt-get install -y pgbouncer
sudo systemctl stop pgbouncer

sudo install -o postgres -g postgres -m 0750 -d /etc/pgbouncer
sudo install -o postgres -g postgres -m 0640 testsetup/pgbouncer.ini /etc/pgbouncer/pgbouncer.ini
sudo install -o postgres -g postgres -m 0640 testsetup/pgbouncer-userlist.txt /etc/pgbouncer/userlist.txt
sudo -u postgres pgbouncer -d /etc/pgbouncer/pgbouncer.ini

for _ in {1..30}; do
  if PGPASSWORD=secret psql "host=127.0.0.1 port=6432 user=pgx_md5 dbname=pgx_test" -c "select 1"; then
    exit 0
  fi
  sleep 1
done

sudo cat /var/log/postgresql/pgbouncer.log || true
exit 1
