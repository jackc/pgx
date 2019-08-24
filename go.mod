module github.com/jackc/pgtype

go 1.12

require (
	github.com/jackc/pgio v1.0.0
	github.com/jackc/pgx/v4 v4.0.0-20190421002000-1b8f0016e912
	github.com/kr/pretty v0.1.0 // indirect
	github.com/lib/pq v1.1.0
	github.com/satori/go.uuid v1.2.0
	github.com/shopspring/decimal v0.0.0-20180709203117-cd690d0c9e24
	github.com/stretchr/testify v1.4.0
	go.uber.org/multierr v1.1.0 // indirect
	golang.org/x/xerrors v0.0.0-20190717185122-a985d3407aa7
	gopkg.in/check.v1 v1.0.0-20180628173108-788fd7840127 // indirect
)

replace github.com/jackc/pgx/v4 => ../pgx
