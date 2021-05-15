module github.com/jackc/pgx/v4/examples

go 1.15

replace (
	github.com/jackc/pgx/v4 => ../
	github.com/jackc/pgx/v4/log/log15adapter => ../log/log15adapter
	github.com/jackc/pgx/v4/pgxpool => ../pgxpool/
)

require (
	github.com/go-stack/stack v1.8.0 // indirect
	github.com/jackc/pgx/v4 v4.11.0
	github.com/jackc/pgx/v4/log/log15adapter v0.0.0-00010101000000-000000000000
	github.com/jackc/pgx/v4/pgxpool v0.0.0-00010101000000-000000000000
	github.com/mattn/go-colorable v0.1.8 // indirect
	gopkg.in/inconshreveable/log15.v2 v2.0.0-20200109203555-b30bc20e4fd1
)
