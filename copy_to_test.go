package pgx_test

import (
	"bytes"
	"testing"

	"github.com/jackc/pgx"
)

func TestConnCopyToWriterSmall(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	mustExec(t, conn, `create temporary table foo(
		a int2,
		b int4,
		c int8,
		d varchar,
		e text,
		f date,
		g json
	)`)
	mustExec(t, conn, `insert into foo values (0, 1, 2, 'abc', 'efg', '2000-01-01', '{"abc":"def","foo":"bar"}')`)
	mustExec(t, conn, `insert into foo values (null, null, null, null, null, null, null)`)

	inputBytes := []byte("0\t1\t2\tabc\tefg\t2000-01-01\t{\"abc\":\"def\",\"foo\":\"bar\"}\n" +
		"\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\n")

	outputWriter := bytes.NewBuffer(make([]byte, 0, len(inputBytes)))

	res, err := conn.CopyToWriter(outputWriter, "copy foo to stdout")
	if err != nil {
		t.Errorf("Unexpected error for CopyToWriter: %v", err)
	}

	copyCount := int(res.RowsAffected())
	if copyCount != 2 {
		t.Errorf("Expected CopyToWriter to return 2 copied rows, but got %d", copyCount)
	}

	if i := bytes.Compare(inputBytes, outputWriter.Bytes()); i != 0 {
		t.Errorf("Input rows and output rows do not equal:\n%q\n%q", string(inputBytes), string(outputWriter.Bytes()))
	}

	ensureConnValid(t, conn)
}

func TestConnCopyToWriterLarge(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	mustExec(t, conn, `create temporary table foo(
		a int2,
		b int4,
		c int8,
		d varchar,
		e text,
		f date,
		g json,
		h bytea
	)`)
	inputBytes := make([]byte, 0)

	for i := 0; i < 1000; i++ {
		mustExec(t, conn, `insert into foo values (0, 1, 2, 'abc', 'efg', '2000-01-01', '{"abc":"def","foo":"bar"}', 'oooo')`)
		inputBytes = append(inputBytes, "0\t1\t2\tabc\tefg\t2000-01-01\t{\"abc\":\"def\",\"foo\":\"bar\"}\t\\\\x6f6f6f6f\n"...)
	}

	outputWriter := bytes.NewBuffer(make([]byte, 0, len(inputBytes)))

	res, err := conn.CopyToWriter(outputWriter, "copy foo to stdout")
	if err != nil {
		t.Errorf("Unexpected error for CopyFrom: %v", err)
	}

	copyCount := int(res.RowsAffected())
	if copyCount != 1000 {
		t.Errorf("Expected CopyToWriter to return 1 copied rows, but got %d", copyCount)
	}

	if i := bytes.Compare(inputBytes, outputWriter.Bytes()); i != 0 {
		t.Errorf("Input rows and output rows do not equal")
	}

	ensureConnValid(t, conn)
}

func TestConnCopyToWriterQueryError(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	outputWriter := bytes.NewBuffer(make([]byte, 0))

	res, err := conn.CopyToWriter(outputWriter, "cropy foo to stdout")
	if err == nil {
		t.Errorf("Expected CopyToWriter return error, but it did not")
	}

	if _, ok := err.(pgx.PgError); !ok {
		t.Errorf("Expected CopyToWriter return pgx.PgError, but instead it returned: %v", err)
	}

	copyCount := int(res.RowsAffected())
	if copyCount != 0 {
		t.Errorf("Expected CopyToWriter to return 0 copied rows, but got %d", copyCount)
	}

	ensureConnValid(t, conn)
}
