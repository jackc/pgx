package pgxpool

import (
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type poolBatchResults struct {
	br pgx.BatchResults
	c  *Conn
	tx *Tx // owns c instead when the batch began a transaction
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

func (br *poolBatchResults) Tx() pgx.Tx {
	if br.tx == nil {
		return nil
	}
	return br.tx
}
