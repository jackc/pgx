FROM mcr.microsoft.com/devcontainers/base:ubuntu24.04

# The PostgreSQL CLIENT plus the SERVER packages for every major pgx tests against, from the
# PostgreSQL project's own repository.
#
# The server packages (initdb / postgres / pg_ctl) are here so the container can run the same
# per-checkout clusters under .dev that a native macOS or Linux checkout does — the convergence
# this environment is built on. They do NOT start any service: Debian's packaging would otherwise
# auto-create and start a machine-wide "main" cluster for each major, which is precisely what the
# per-checkout model replaces. create_main_cluster must be set BEFORE the packages are installed,
# since the cluster is created by their post-install script.
#
# The client is 18, the newest major: libpq is backward compatible, so one psql and one pg_isready
# serve all five servers.
#
# The grep either side of the sed is not belt-and-braces. `sed -i s/.../.../` exits 0 when it
# matches nothing, so a postgresql-common that renamed or dropped that key would leave the setting
# untouched and the very next apt-get would create five machine-wide clusters on 5432-5436 — the
# thing this comment says must not happen — with no error anywhere in the build log. Asserting the
# key exists first and holds the intended value afterwards makes that failure loud.
RUN apt-get update && apt-get install -y postgresql-common \
    && yes | /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh \
    && grep -qE '^#*\s*create_main_cluster' /etc/postgresql-common/createcluster.conf \
    && sed -i 's/^#*\s*create_main_cluster.*/create_main_cluster = false/' \
        /etc/postgresql-common/createcluster.conf \
    && grep -qx 'create_main_cluster = false' /etc/postgresql-common/createcluster.conf \
    && apt-get install -y postgresql-client-18 \
        postgresql-14 postgresql-15 postgresql-16 postgresql-17 postgresql-18 \
        postgresql-contrib-14 postgresql-contrib-15 postgresql-contrib-16 \
        postgresql-contrib-17 postgresql-contrib-18 \
    && rm -rf /var/lib/apt/lists/*

# Build dependencies for the toolchain mise provisions from source.
RUN apt-get update \
    && apt-get install -y build-essential libssl-dev libreadline-dev zlib1g-dev libffi-dev \
        ncurses-dev libyaml-dev libxml2 libxml2-dev libxslt1-dev \
    && rm -rf /var/lib/apt/lists/*
