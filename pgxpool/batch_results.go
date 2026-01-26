package pgxpool

import (
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type poolBatchResults struct {
	br pgx.BatchResults
	c  *Conn
}

func PoolBatchResults(br pgx.BatchResults, c *Conn) pgx.BatchResults {
	return &poolBatchResults{br: br, c: c}
}

func (br *poolBatchResults) Exec() (pgconn.CommandTag, error) {
	return br.br.Exec()
}

func (br *poolBatchResults) Query() (pgx.Rows, error) {
	return br.br.Query()
}

func (br *poolBatchResults) QueryRow() pgx.Row {
	return br.br.QueryRow()
}

func (br *poolBatchResults) Close() error {
	err := br.br.Close()
	if br.c != nil {
		br.c.Release()
		br.c = nil
	}
	return err
}
