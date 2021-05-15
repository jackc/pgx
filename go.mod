module github.com/jackc/pgx/v4

go 1.15

replace (
	github.com/jackc/pgx/v4 v4.11.0 => ./
	github.com/jackc/pgx/v4/examples => ./examples
	github.com/jackc/pgx/v4/log => ./log
)

require (
	github.com/Masterminds/semver/v3 v3.1.1
	github.com/cockroachdb/apd v1.1.0
	github.com/gofrs/uuid v4.0.0+incompatible
	github.com/jackc/pgconn v1.8.1
	github.com/jackc/pgio v1.0.0
	github.com/jackc/pgproto3/v2 v2.0.7
	github.com/jackc/pgtype v1.7.0
	github.com/shopspring/decimal v1.2.0
	github.com/stretchr/testify v1.7.0
)
