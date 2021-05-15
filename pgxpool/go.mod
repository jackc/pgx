module github.com/jackc/pgx/v4/pgxpool

go 1.15

replace github.com/jackc/pgx/v4 v4.11.0 => ../

require (
	github.com/jackc/pgconn v1.8.1
	github.com/jackc/pgproto3/v2 v2.0.7
	github.com/jackc/pgx/v4 v4.11.0
	github.com/jackc/puddle v1.1.3
	github.com/stretchr/testify v1.7.0
)
