package pgx_test

import (
	"context"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	errors "golang.org/x/xerrors"
)

func TestConnCopyFromSmall(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	mustExec(t, conn, `create temporary table foo(
		a int2,
		b int4,
		c int8,
		d varchar,
		e text,
		f date,
		g timestamptz
	)`)

	tzedTime := time.Date(2010, 2, 3, 4, 5, 6, 0, time.Local)

	inputRows := [][]interface{}{
		{int16(0), int32(1), int64(2), "abc", "efg", time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), tzedTime},
		{nil, nil, nil, nil, nil, nil, nil},
	}

	copyCount, err := conn.CopyFrom(context.Background(), pgx.Identifier{"foo"}, []string{"a", "b", "c", "d", "e", "f", "g"}, pgx.CopyFromRows(inputRows))
	if err != nil {
		t.Errorf("Unexpected error for CopyFrom: %v", err)
	}
	if int(copyCount) != len(inputRows) {
		t.Errorf("Expected CopyFrom to return %d copied rows, but got %d", len(inputRows), copyCount)
	}

	rows, err := conn.Query(context.Background(), "select * from foo")
	if err != nil {
		t.Errorf("Unexpected error for Query: %v", err)
	}

	var outputRows [][]interface{}
	for rows.Next() {
		row, err := rows.Values()
		if err != nil {
			t.Errorf("Unexpected error for rows.Values(): %v", err)
		}
		outputRows = append(outputRows, row)
	}

	if rows.Err() != nil {
		t.Errorf("Unexpected error for rows.Err(): %v", rows.Err())
	}

	if !reflect.DeepEqual(inputRows, outputRows) {
		t.Errorf("Input rows and output rows do not equal: %v -> %v", inputRows, outputRows)
	}

	ensureConnValid(t, conn)
}

func TestConnCopyFromLarge(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	mustExec(t, conn, `create temporary table foo(
		a int2,
		b int4,
		c int8,
		d varchar,
		e text,
		f date,
		g timestamptz,
		h bytea
	)`)

	tzedTime := time.Date(2010, 2, 3, 4, 5, 6, 0, time.Local)

	inputRows := [][]interface{}{}

	for i := 0; i < 10000; i++ {
		inputRows = append(inputRows, []interface{}{int16(0), int32(1), int64(2), "abc", "efg", time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), tzedTime, []byte{111, 111, 111, 111}})
	}

	copyCount, err := conn.CopyFrom(context.Background(), pgx.Identifier{"foo"}, []string{"a", "b", "c", "d", "e", "f", "g", "h"}, pgx.CopyFromRows(inputRows))
	if err != nil {
		t.Errorf("Unexpected error for CopyFrom: %v", err)
	}
	if int(copyCount) != len(inputRows) {
		t.Errorf("Expected CopyFrom to return %d copied rows, but got %d", len(inputRows), copyCount)
	}

	rows, err := conn.Query(context.Background(), "select * from foo")
	if err != nil {
		t.Errorf("Unexpected error for Query: %v", err)
	}

	var outputRows [][]interface{}
	for rows.Next() {
		row, err := rows.Values()
		if err != nil {
			t.Errorf("Unexpected error for rows.Values(): %v", err)
		}
		outputRows = append(outputRows, row)
	}

	if rows.Err() != nil {
		t.Errorf("Unexpected error for rows.Err(): %v", rows.Err())
	}

	if !reflect.DeepEqual(inputRows, outputRows) {
		t.Errorf("Input rows and output rows do not equal")
	}

	ensureConnValid(t, conn)
}

func TestConnCopyFromJSON(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	for _, typeName := range []string{"json", "jsonb"} {
		if _, ok := conn.ConnInfo().DataTypeForName(typeName); !ok {
			return // No JSON/JSONB type -- must be running against old PostgreSQL
		}
	}

	mustExec(t, conn, `create temporary table foo(
		a json,
		b jsonb
	)`)

	inputRows := [][]interface{}{
		{map[string]interface{}{"foo": "bar"}, map[string]interface{}{"bar": "quz"}},
		{nil, nil},
	}

	copyCount, err := conn.CopyFrom(context.Background(), pgx.Identifier{"foo"}, []string{"a", "b"}, pgx.CopyFromRows(inputRows))
	if err != nil {
		t.Errorf("Unexpected error for CopyFrom: %v", err)
	}
	if int(copyCount) != len(inputRows) {
		t.Errorf("Expected CopyFrom to return %d copied rows, but got %d", len(inputRows), copyCount)
	}

	rows, err := conn.Query(context.Background(), "select * from foo")
	if err != nil {
		t.Errorf("Unexpected error for Query: %v", err)
	}

	var outputRows [][]interface{}
	for rows.Next() {
		row, err := rows.Values()
		if err != nil {
			t.Errorf("Unexpected error for rows.Values(): %v", err)
		}
		outputRows = append(outputRows, row)
	}

	if rows.Err() != nil {
		t.Errorf("Unexpected error for rows.Err(): %v", rows.Err())
	}

	if !reflect.DeepEqual(inputRows, outputRows) {
		t.Errorf("Input rows and output rows do not equal: %v -> %v", inputRows, outputRows)
	}

	ensureConnValid(t, conn)
}

type clientFailSource struct {
	count int
	err   error
}

func (cfs *clientFailSource) Next() bool {
	cfs.count++
	return cfs.count < 100
}

func (cfs *clientFailSource) Values() ([]interface{}, error) {
	if cfs.count == 3 {
		cfs.err = errors.Errorf("client error")
		return nil, cfs.err
	}
	return []interface{}{make([]byte, 100000)}, nil
}

func (cfs *clientFailSource) Err() error {
	return cfs.err
}

func TestConnCopyFromFailServerSideMidway(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	mustExec(t, conn, `create temporary table foo(
		a int4,
		b varchar not null
	)`)

	inputRows := [][]interface{}{
		{int32(1), "abc"},
		{int32(2), nil}, // this row should trigger a failure
		{int32(3), "def"},
	}

	copyCount, err := conn.CopyFrom(context.Background(), pgx.Identifier{"foo"}, []string{"a", "b"}, pgx.CopyFromRows(inputRows))
	if err == nil {
		t.Errorf("Expected CopyFrom return error, but it did not")
	}
	if _, ok := err.(*pgconn.PgError); !ok {
		t.Errorf("Expected CopyFrom return pgx.PgError, but instead it returned: %v", err)
	}
	if copyCount != 0 {
		t.Errorf("Expected CopyFrom to return 0 copied rows, but got %d", copyCount)
	}

	rows, err := conn.Query(context.Background(), "select * from foo")
	if err != nil {
		t.Errorf("Unexpected error for Query: %v", err)
	}

	var outputRows [][]interface{}
	for rows.Next() {
		row, err := rows.Values()
		if err != nil {
			t.Errorf("Unexpected error for rows.Values(): %v", err)
		}
		outputRows = append(outputRows, row)
	}

	if rows.Err() != nil {
		t.Errorf("Unexpected error for rows.Err(): %v", rows.Err())
	}

	if len(outputRows) != 0 {
		t.Errorf("Expected 0 rows, but got %v", outputRows)
	}

	mustExec(t, conn, "truncate foo")

	ensureConnValid(t, conn)
}

type failSource struct {
	count int
}

func (fs *failSource) Next() bool {
	time.Sleep(time.Millisecond * 100)
	fs.count++
	return fs.count < 100
}

func (fs *failSource) Values() ([]interface{}, error) {
	if fs.count == 3 {
		return []interface{}{nil}, nil
	}
	return []interface{}{make([]byte, 100000)}, nil
}

func (fs *failSource) Err() error {
	return nil
}

func TestConnCopyFromFailServerSideMidwayAbortsWithoutWaiting(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	mustExec(t, conn, `create temporary table foo(
		a bytea not null
	)`)

	startTime := time.Now()

	copyCount, err := conn.CopyFrom(context.Background(), pgx.Identifier{"foo"}, []string{"a"}, &failSource{})
	if err == nil {
		t.Errorf("Expected CopyFrom return error, but it did not")
	}
	if _, ok := err.(*pgconn.PgError); !ok {
		t.Errorf("Expected CopyFrom return pgx.PgError, but instead it returned: %v", err)
	}
	if copyCount != 0 {
		t.Errorf("Expected CopyFrom to return 0 copied rows, but got %d", copyCount)
	}

	endTime := time.Now()
	copyTime := endTime.Sub(startTime)
	if copyTime > time.Second {
		t.Errorf("Failing CopyFrom shouldn't have taken so long: %v", copyTime)
	}

	rows, err := conn.Query(context.Background(), "select * from foo")
	if err != nil {
		t.Errorf("Unexpected error for Query: %v", err)
	}

	var outputRows [][]interface{}
	for rows.Next() {
		row, err := rows.Values()
		if err != nil {
			t.Errorf("Unexpected error for rows.Values(): %v", err)
		}
		outputRows = append(outputRows, row)
	}

	if rows.Err() != nil {
		t.Errorf("Unexpected error for rows.Err(): %v", rows.Err())
	}

	if len(outputRows) != 0 {
		t.Errorf("Expected 0 rows, but got %v", outputRows)
	}

	ensureConnValid(t, conn)
}

func TestConnCopyFromCopyFromSourceErrorMidway(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	mustExec(t, conn, `create temporary table foo(
		a bytea not null
	)`)

	copyCount, err := conn.CopyFrom(context.Background(), pgx.Identifier{"foo"}, []string{"a"}, &clientFailSource{})
	if err == nil {
		t.Errorf("Expected CopyFrom return error, but it did not")
	}
	if copyCount != 0 {
		t.Errorf("Expected CopyFrom to return 0 copied rows, but got %d", copyCount)
	}

	rows, err := conn.Query(context.Background(), "select * from foo")
	if err != nil {
		t.Errorf("Unexpected error for Query: %v", err)
	}

	var outputRows [][]interface{}
	for rows.Next() {
		row, err := rows.Values()
		if err != nil {
			t.Errorf("Unexpected error for rows.Values(): %v", err)
		}
		outputRows = append(outputRows, row)
	}

	if rows.Err() != nil {
		t.Errorf("Unexpected error for rows.Err(): %v", rows.Err())
	}

	if len(outputRows) != 0 {
		t.Errorf("Expected 0 rows, but got %v", len(outputRows))
	}

	ensureConnValid(t, conn)
}

type clientFinalErrSource struct {
	count int
}

func (cfs *clientFinalErrSource) Next() bool {
	cfs.count++
	return cfs.count < 5
}

func (cfs *clientFinalErrSource) Values() ([]interface{}, error) {
	return []interface{}{make([]byte, 100000)}, nil
}

func (cfs *clientFinalErrSource) Err() error {
	return errors.Errorf("final error")
}

func TestConnCopyFromCopyFromSourceErrorEnd(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	mustExec(t, conn, `create temporary table foo(
		a bytea not null
	)`)

	copyCount, err := conn.CopyFrom(context.Background(), pgx.Identifier{"foo"}, []string{"a"}, &clientFinalErrSource{})
	if err == nil {
		t.Errorf("Expected CopyFrom return error, but it did not")
	}
	if copyCount != 0 {
		t.Errorf("Expected CopyFrom to return 0 copied rows, but got %d", copyCount)
	}

	rows, err := conn.Query(context.Background(), "select * from foo")
	if err != nil {
		t.Errorf("Unexpected error for Query: %v", err)
	}

	var outputRows [][]interface{}
	for rows.Next() {
		row, err := rows.Values()
		if err != nil {
			t.Errorf("Unexpected error for rows.Values(): %v", err)
		}
		outputRows = append(outputRows, row)
	}

	if rows.Err() != nil {
		t.Errorf("Unexpected error for rows.Err(): %v", rows.Err())
	}

	if len(outputRows) != 0 {
		t.Errorf("Expected 0 rows, but got %v", outputRows)
	}

	ensureConnValid(t, conn)
}

func TestConnCopyFromWithNoCustomQueryConflict(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	mustExec(t, conn, `create temporary table foo(
		a int4,
		b varchar not null,
		UNIQUE(a)
	)`)

	inputRows := [][]interface{}{
		{int32(1), "abc"},
		{int32(2), "def"},
		{int32(3), "ghi"},
	}

	duplicateRows := [][]interface{}{
		{int32(2), "efg"},
	}

	copyCount, err := conn.CopyFrom(context.Background(), pgx.Identifier{"foo"}, []string{"a", "b"}, pgx.CopyFromRows(append(inputRows, duplicateRows...)))
	if pgErr, ok := err.(*pgconn.PgError); !ok || pgErr.Code != "23505" {
		t.Errorf("Unexpected error for CopyFrom: %v", err)
	}
	if int(copyCount) != 0 {
		t.Errorf("Expected CopyFrom to return %d copied rows, but got %d", 0, copyCount)
	}
}

func TestConnCopyFromWithCustomQueryAndConflict(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	mustExec(t, conn, `create temporary table foo(
		a int4,
		b varchar not null,
		UNIQUE(a)
	)`)

	inputRows := [][]interface{}{
		{int32(1), "abc"},
		{int32(2), "def"},
		{int32(3), "ghi"},
	}

	duplicateRows := [][]interface{}{
		{int32(2), "efg"},
	}

	copyCount, err := conn.CopyFrom(context.Background(), pgx.Identifier{"foo"}, []string{"a", "b"}, pgx.CopyFromRows(append(inputRows, duplicateRows...)), pgx.WithQueryTemplate(`CREATE TEMP TABLE tmp_table
		ON COMMIT DROP
		AS
		SELECT *
		FROM {{ .TableName }}
		WITH NO DATA;

		COPY tmp_table ({{ .ColumnNames }}) FROM stdin binary;

		INSERT INTO {{ .TableName }}
			SELECT * FROM tmp_table
		ON CONFLICT DO NOTHING;`))

	if err != nil {
		t.Errorf("Unexpected error for CopyFrom: %v", err)
	}
	if int(copyCount) != len(inputRows) {
		t.Errorf("Expected CopyFrom to return %d copied rows, but got %d", len(inputRows), copyCount)
	}

	rows, err := conn.Query(context.Background(), "select * from foo")
	if err != nil {
		t.Errorf("Unexpected error for Query: %v", err)
	}

	var outputRows [][]interface{}
	for rows.Next() {
		row, err := rows.Values()
		if err != nil {
			t.Errorf("Unexpected error for rows.Values(): %v", err)
		}
		outputRows = append(outputRows, row)
	}

	if rows.Err() != nil {
		t.Errorf("Unexpected error for rows.Err(): %v", rows.Err())
	}

	if !reflect.DeepEqual(inputRows, outputRows) {
		t.Errorf("Input rows and output rows do not equal: %v -> %v", inputRows, outputRows)
	}

	ensureConnValid(t, conn)
}
