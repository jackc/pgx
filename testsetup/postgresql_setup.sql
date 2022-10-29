-- Create extensions and types.
create extension hstore;
create domain uint64 as numeric(20,0);

-- Create users for different types of connections and authentication.
create user pgx_ssl PASSWORD 'secret' with superuser;
set password_encryption = md5;
create user pgx_md5 PASSWORD 'secret' with superuser;
set password_encryption = 'scram-sha-256';
create user pgx_pw PASSWORD 'secret' with superuser;
create user pgx_scram PASSWORD 'secret' with superuser;
\set whoami `whoami`
create user :whoami with superuser; -- unix domain socket user


-- The tricky test user, below, has to actually exist so that it can be used in a test
-- of aclitem formatting. It turns out aclitems cannot contain non-existing users/roles.
create user " tricky, ' } "" \\ test user " superuser password 'secret';
