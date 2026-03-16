-- Create extensions and types.
create extension hstore;
create extension ltree;
create domain uint64 as numeric(20,0);

-- Create users for different types of connections and authentication.
create user pgx_ssl with superuser PASSWORD 'secret';
create user pgx_sslcert with superuser PASSWORD 'secret';
set password_encryption = md5;
create user pgx_md5 with superuser PASSWORD 'secret';
set password_encryption = 'scram-sha-256';
create user pgx_pw with superuser PASSWORD 'secret';
create user pgx_scram with superuser PASSWORD 'secret';
create user pgx_oauth with superuser;

-- When running in devcontainers, `whoami` will be `postgres`. Since the
-- `postgres` user already exists, attempting to recreate it will fail.
-- Therefore, we'll guard against that by no-op'ing if/when the user already
-- exists and thereby not aborting the remaining setup.
\set whoami `whoami`
select format('create user %I with superuser', :'whoami')
where not exists (select from pg_roles where rolname = :'whoami') \gexec

-- The tricky test user, below, has to actually exist so that it can be used in a test
-- of aclitem formatting. It turns out aclitems cannot contain non-existing users/roles.
create user " tricky, ' } "" \\ test user " superuser password 'secret';
