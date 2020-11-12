module github.com/jackc/pgx/v4

go 1.12

require (
	github.com/cockroachdb/apd v1.1.0
	github.com/gofrs/uuid v3.2.0+incompatible
	github.com/jackc/pgconn v1.7.2
	github.com/jackc/pgio v1.0.0
	github.com/jackc/pgproto3/v2 v2.0.6
	github.com/jackc/pgtype v1.6.1
	github.com/jackc/puddle v1.1.2
	github.com/rs/zerolog v1.15.0
	github.com/shopspring/decimal v0.0.0-20200227202807-02e2044944cc
	github.com/sirupsen/logrus v1.4.2
	github.com/stretchr/testify v1.5.1
	go.uber.org/zap v1.10.0
	golang.org/x/xerrors v0.0.0-20191204190536-9bdfabe68543
	gopkg.in/inconshreveable/log15.v2 v2.0.0-20180818164646-67afb5ed74ec
)

replace github.com/jackc/pgconn => github.com/vivacitylabs/pgconn v1.7.3-0.20201112061553-4c9718b45e2d
