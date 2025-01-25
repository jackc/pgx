#!/usr/bin/env bash
set -eux

if [[ "${PGVERSION-}" =~ ^[0-9.]+$ ]]
then
  sudo apt-get remove -y --purge postgresql libpq-dev libpq5 postgresql-client-common postgresql-common
  sudo rm -rf /var/lib/postgresql
  wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
  sudo sh -c "echo deb http://apt.postgresql.org/pub/repos/apt/ $(lsb_release -cs)-pgdg main $PGVERSION >> /etc/apt/sources.list.d/postgresql.list"
  sudo apt-get update -qq
  sudo apt-get -y -o Dpkg::Options::=--force-confdef -o Dpkg::Options::="--force-confnew" install postgresql-$PGVERSION postgresql-server-dev-$PGVERSION postgresql-contrib-$PGVERSION

  sudo cp testsetup/pg_hba.conf /etc/postgresql/$PGVERSION/main/pg_hba.conf
  sudo sh -c "echo \"listen_addresses = '127.0.0.1'\" >> /etc/postgresql/$PGVERSION/main/postgresql.conf"
  sudo sh -c "cat testsetup/postgresql_ssl.conf >> /etc/postgresql/$PGVERSION/main/postgresql.conf"

  cd testsetup

  # Generate CA, server, and encrypted client certificates.
  go run generate_certs.go

  # Copy certificates to server directory and set permissions.
  sudo cp ca.pem /var/lib/postgresql/$PGVERSION/main/root.crt
  sudo chown postgres:postgres /var/lib/postgresql/$PGVERSION/main/root.crt
  sudo cp localhost.key /var/lib/postgresql/$PGVERSION/main/server.key
  sudo chown postgres:postgres /var/lib/postgresql/$PGVERSION/main/server.key
  sudo chmod 600 /var/lib/postgresql/$PGVERSION/main/server.key
  sudo cp localhost.crt /var/lib/postgresql/$PGVERSION/main/server.crt
  sudo chown postgres:postgres /var/lib/postgresql/$PGVERSION/main/server.crt

  cp ca.pem /tmp
  cp pgx_sslcert.key /tmp
  cp pgx_sslcert.crt /tmp

  cd ..

  sudo /etc/init.d/postgresql restart

  createdb -U postgres pgx_test
  psql -U postgres -f testsetup/postgresql_setup.sql pgx_test
fi

if [[ "${PGVERSION-}" =~ ^cockroach ]]
then
  wget -qO- https://binaries.cockroachdb.com/cockroach-v24.3.3.linux-amd64.tgz | tar xvz
  sudo mv cockroach-v24.3.3.linux-amd64/cockroach /usr/local/bin/
  cockroach start-single-node --insecure --background --listen-addr=localhost
  cockroach sql --insecure -e 'create database pgx_test'
fi

if [ "${CRATEVERSION-}" != "" ]
then
  docker run \
    -p "6543:5432" \
    -d \
    crate:"$CRATEVERSION" \
    crate \
      -Cnetwork.host=0.0.0.0 \
      -Ctransport.host=localhost \
      -Clicense.enterprise=false
fi
