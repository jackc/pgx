#!/usr/bin/env bash
set -eux

go get -u github.com/cockroachdb/apd
go get -u github.com/shopspring/decimal
go get -u gopkg.in/inconshreveable/log15.v2
go get -u github.com/gofrs/uuid
go get -u github.com/jackc/fake
go get -u github.com/lib/pq
go get -u github.com/hashicorp/go-version
go get -u github.com/satori/go.uuid
go get -u github.com/sirupsen/logrus
go get -u github.com/pkg/errors
go get -u go.uber.org/zap
go get -u github.com/rs/zerolog
go get -u golang.org/x/crypto/pbkdf2
go get -u golang.org/x/text/secure/precis
