module github.com/jackc/pgx/v4/log/logrusadapter

go 1.15

replace github.com/jackc/pgx/v4 v4.11.0 => ../../

require (
	github.com/jackc/pgx/v4 v4.11.0
	github.com/sirupsen/logrus v1.8.1
)
