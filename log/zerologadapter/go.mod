module github.com/jackc/pgx/v4/log/zerologadapter

go 1.15

replace github.com/jackc/pgx/v4 v4.11.0 => ../../

require (
	github.com/jackc/pgx/v4 v4.11.0
	github.com/rs/zerolog v1.22.0
)
