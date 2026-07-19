#!/usr/bin/env bash
set -euxo pipefail

: "${PGBOUNCER_VERSION:?PGBOUNCER_VERSION must be set}"
: "${PGBOUNCER_SHA256:?PGBOUNCER_SHA256 must be set}"

sudo apt-get install -y build-essential curl libevent-dev libssl-dev pkg-config

pgbouncer_build_dir="$(mktemp -d)"
pgbouncer_archive="$pgbouncer_build_dir/pgbouncer-$PGBOUNCER_VERSION.tar.gz"
curl --fail --location --silent --show-error \
  "https://www.pgbouncer.org/downloads/files/$PGBOUNCER_VERSION/pgbouncer-$PGBOUNCER_VERSION.tar.gz" \
  --output "$pgbouncer_archive"
[[ "$(sha256sum "$pgbouncer_archive" | cut -d ' ' -f 1)" == "$PGBOUNCER_SHA256" ]]
tar -xzf "$pgbouncer_archive" -C "$pgbouncer_build_dir"

(
  cd "$pgbouncer_build_dir/pgbouncer-$PGBOUNCER_VERSION"
  ./configure --prefix=/usr/local
  make -j2 pgbouncer
  sudo install -m 0755 pgbouncer /usr/local/bin/pgbouncer
)

/usr/local/bin/pgbouncer --version | grep -Fqx "PgBouncer $PGBOUNCER_VERSION"

sudo install -o postgres -g postgres -m 0750 -d /etc/pgbouncer
sudo install -o postgres -g postgres -m 0640 testsetup/pgbouncer.ini /etc/pgbouncer/pgbouncer.ini
sudo install -o postgres -g postgres -m 0640 testsetup/pgbouncer-userlist.txt /etc/pgbouncer/userlist.txt
sudo -u postgres /usr/local/bin/pgbouncer -d /etc/pgbouncer/pgbouncer.ini

for _ in {1..30}; do
  if PGPASSWORD=secret psql "host=127.0.0.1 port=6432 user=pgx_md5 dbname=pgx_test" -c "select 1"; then
    exit 0
  fi
  sleep 1
done

sudo cat /var/log/postgresql/pgbouncer.log || true
exit 1
