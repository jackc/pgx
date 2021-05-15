module github.com/jackc/pgx/v4/log/zapadapter

go 1.15

replace github.com/jackc/pgx/v4 v4.11.0 => ../../

require (
	github.com/jackc/pgx/v4 v4.11.0
	go.uber.org/zap v1.16.0
)
