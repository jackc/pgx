package pool

import (
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
)

type errBatchResults struct {
	err error
}

func (br errBatchResults) ExecResults() (pgconn.CommandTag, error) {
	return nil, br.err
}

func (br errBatchResults) QueryResults() (pgx.Rows, error) {
	return errRows{err: br.err}, br.err
}

func (br errBatchResults) QueryRowResults() pgx.Row {
	return errRow{err: br.err}
}

func (br errBatchResults) Close() error {
	return br.err
}

type poolBatchResults struct {
	br pgx.BatchResults
	c  *Conn
}

func (br *poolBatchResults) ExecResults() (pgconn.CommandTag, error) {
	return br.br.ExecResults()
}

func (br *poolBatchResults) QueryResults() (pgx.Rows, error) {
	return br.br.QueryResults()
}

func (br *poolBatchResults) QueryRowResults() pgx.Row {
	return br.br.QueryRowResults()
}

func (br *poolBatchResults) Close() error {
	err := br.br.Close()
	if br.c != nil {
		br.c.Release()
		br.c = nil
	}
	return err
}
