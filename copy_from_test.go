package pgx_test

import (
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx"
	"github.com/pkg/errors"
)

func TestConnCopyFromSmall(t *testing.T) {
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
		g timestamptz
	)`)

	tzedTime := time.Date(2010, 2, 3, 4, 5, 6, 0, time.Local)

	inputRows := [][]interface{}{
		{int16(0), int32(1), int64(2), "abc", "efg", time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), tzedTime},
		{nil, nil, nil, nil, nil, nil, nil},
	}
	inputReader := strings.NewReader("0\t1\t2\tabc\tefg\t2000-01-01\t" + tzedTime.Format(time.RFC3339Nano) + "\n" +
		"\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\n")

	copyCount, err := conn.CopyFrom(pgx.Identifier{"foo"}, []string{"a", "b", "c", "d", "e", "f", "g"}, pgx.CopyFromRows(inputRows))
	if err != nil {
		t.Errorf("Unexpected error for CopyFrom: %v", err)
	}
	if copyCount != len(inputRows) {
		t.Errorf("Expected CopyFrom to return %d copied rows, but got %d", len(inputRows), copyCount)
	}

	rows, err := conn.Query("select * from foo")
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

	mustExec(t, conn, "truncate foo")

	res, err := conn.CopyFromReader(inputReader, "copy foo from stdin")
	if err != nil {
		t.Errorf("Unexpected error for CopyFromReader: %v", err)
	}
	copyCount = int(res.RowsAffected())
	if copyCount != len(inputRows) {
		t.Errorf("Expected CopyFromReader to return %d copied rows, but got %d", len(inputRows), copyCount)
	}

	rows, err = conn.Query("select * from foo")
	if err != nil {
		t.Errorf("Unexpected error for Query: %v", err)
	}

	outputRows = make([][]interface{}, 0)
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

	conn := mustConnect(t, *defaultConnConfig)
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
	inputStringRows := ""

	for i := 0; i < 10000; i++ {
		inputRows = append(inputRows, []interface{}{int16(0), int32(1), int64(2), "abc", "efg", time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), tzedTime, []byte{111, 111, 111, 111}})
		inputStringRows += "0\t1\t2\tabc\tefg\t2000-01-01\t" + tzedTime.Format(time.RFC3339Nano) + "\toooo\n"
	}

	copyCount, err := conn.CopyFrom(pgx.Identifier{"foo"}, []string{"a", "b", "c", "d", "e", "f", "g", "h"}, pgx.CopyFromRows(inputRows))
	if err != nil {
		t.Errorf("Unexpected error for CopyFrom: %v", err)
	}
	if copyCount != len(inputRows) {
		t.Errorf("Expected CopyFrom to return %d copied rows, but got %d", len(inputRows), copyCount)
	}

	rows, err := conn.Query("select * from foo")
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

	mustExec(t, conn, "truncate foo")

	res, err := conn.CopyFromReader(strings.NewReader(inputStringRows), "copy foo from stdin")
	if err != nil {
		t.Errorf("Unexpected error for CopyFromReader: %v", err)
	}
	copyCount = int(res.RowsAffected())
	if copyCount != len(inputRows) {
		t.Errorf("Expected CopyFromReader to return %d copied rows, but got %d", len(inputRows), copyCount)
	}

	rows, err = conn.Query("select * from foo")
	if err != nil {
		t.Errorf("Unexpected error for Query: %v", err)
	}

	outputRows = make([][]interface{}, 0)
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

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	for _, typeName := range []string{"json", "jsonb"} {
		if _, ok := conn.ConnInfo.DataTypeForName(typeName); !ok {
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
	inputReader := strings.NewReader("{\"foo\":\"bar\"}\t{\"bar\":\"quz\"}\n\\N\t\\N\n")

	copyCount, err := conn.CopyFrom(pgx.Identifier{"foo"}, []string{"a", "b"}, pgx.CopyFromRows(inputRows))
	if err != nil {
		t.Errorf("Unexpected error for CopyFrom: %v", err)
	}
	if copyCount != len(inputRows) {
		t.Errorf("Expected CopyFrom to return %d copied rows, but got %d", len(inputRows), copyCount)
	}

	rows, err := conn.Query("select * from foo")
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

	mustExec(t, conn, "truncate foo")

	res, err := conn.CopyFromReader(inputReader, "copy foo from stdin")
	if err != nil {
		t.Errorf("Unexpected error for CopyFrom: %v", err)
	}
	copyCount = int(res.RowsAffected())
	if copyCount != len(inputRows) {
		t.Errorf("Expected CopyFromReader to return %d copied rows, but got %d", len(inputRows), copyCount)
	}

	rows, err = conn.Query("select * from foo")
	if err != nil {
		t.Errorf("Unexpected error for Query: %v", err)
	}

	outputRows = make([][]interface{}, 0)
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

	conn := mustConnect(t, *defaultConnConfig)
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
	inputReader := strings.NewReader("1\tabc\n2\t\\N\n3\tdef\n")

	copyCount, err := conn.CopyFrom(pgx.Identifier{"foo"}, []string{"a", "b"}, pgx.CopyFromRows(inputRows))
	if err == nil {
		t.Errorf("Expected CopyFrom return error, but it did not")
	}
	if _, ok := err.(pgx.PgError); !ok {
		t.Errorf("Expected CopyFrom return pgx.PgError, but instead it returned: %v", err)
	}
	if copyCount != 0 {
		t.Errorf("Expected CopyFrom to return 0 copied rows, but got %d", copyCount)
	}

	rows, err := conn.Query("select * from foo")
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

	res, err := conn.CopyFromReader(inputReader, "copy foo from stdin")
	if err == nil {
		t.Errorf("Expected CopyFromReader return error, but it did not")
	}
	if _, ok := err.(pgx.PgError); !ok {
		t.Errorf("Expected CopyFromReader return pgx.PgError, but instead it returned: %v", err)
	}
	copyCount = int(res.RowsAffected())
	if copyCount != 0 {
		t.Errorf("Expected CopyFromReader to return 0 copied rows, but got %d", copyCount)
	}

	rows, err = conn.Query("select * from foo")
	if err != nil {
		t.Errorf("Unexpected error for Query: %v", err)
	}

	outputRows = make([][]interface{}, 0)
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

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	mustExec(t, conn, `create temporary table foo(
		a bytea not null
	)`)

	startTime := time.Now()

	copyCount, err := conn.CopyFrom(pgx.Identifier{"foo"}, []string{"a"}, &failSource{})
	if err == nil {
		t.Errorf("Expected CopyFrom return error, but it did not")
	}
	if _, ok := err.(pgx.PgError); !ok {
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

	rows, err := conn.Query("select * from foo")
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

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	mustExec(t, conn, `create temporary table foo(
		a bytea not null
	)`)

	copyCount, err := conn.CopyFrom(pgx.Identifier{"foo"}, []string{"a"}, &clientFailSource{})
	if err == nil {
		t.Errorf("Expected CopyFrom return error, but it did not")
	}
	if copyCount != 0 {
		t.Errorf("Expected CopyFrom to return 0 copied rows, but got %d", copyCount)
	}

	rows, err := conn.Query("select * from foo")
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

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	mustExec(t, conn, `create temporary table foo(
		a bytea not null
	)`)

	copyCount, err := conn.CopyFrom(pgx.Identifier{"foo"}, []string{"a"}, &clientFinalErrSource{})
	if err == nil {
		t.Errorf("Expected CopyFrom return error, but it did not")
	}
	if copyCount != 0 {
		t.Errorf("Expected CopyFrom to return 0 copied rows, but got %d", copyCount)
	}

	rows, err := conn.Query("select * from foo")
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

type nextPanicSource struct {
}

func (cfs *nextPanicSource) Next() bool {
	panic("crash")
}

func (cfs *nextPanicSource) Values() ([]interface{}, error) {
	return []interface{}{nil}, nil // should never get here
}

func (cfs *nextPanicSource) Err() error {
	return nil // should never gets here
}

func TestConnCopyFromCopyFromSourceNextPanic(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	mustExec(t, conn, `create temporary table foo(
		a bytea not null
	)`)

	caughtPanic := false

	func() {
		defer func() {
			if x := recover(); x != nil {
				caughtPanic = true
			}
		}()

		conn.CopyFrom(pgx.Identifier{"foo"}, []string{"a"}, &nextPanicSource{})
	}()

	if !caughtPanic {
		t.Error("expected panic but did not")
	}

	if conn.IsAlive() {
		t.Error("panic should have killed conn")
	}
}

func TestConnCopyFromReaderQueryError(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	inputReader := strings.NewReader("")

	res, err := conn.CopyFromReader(inputReader, "cropy foo from stdin")
	if err == nil {
		t.Errorf("Expected CopyFromReader return error, but it did not")
	}

	if _, ok := err.(pgx.PgError); !ok {
		t.Errorf("Expected CopyFromReader return pgx.PgError, but instead it returned: %v", err)
	}

	copyCount := int(res.RowsAffected())
	if copyCount != 0 {
		t.Errorf("Expected CopyFromReader to return 0 copied rows, but got %d", copyCount)
	}

	ensureConnValid(t, conn)
}

func TestConnCopyFromReaderNoTableError(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	inputReader := strings.NewReader("")

	res, err := conn.CopyFromReader(inputReader, "copy foo from stdin")
	if err == nil {
		t.Errorf("Expected CopyFromReader return error, but it did not")
	}

	if _, ok := err.(pgx.PgError); !ok {
		t.Errorf("Expected CopyFromReader return pgx.PgError, but instead it returned: %v", err)
	}

	copyCount := int(res.RowsAffected())
	if copyCount != 0 {
		t.Errorf("Expected CopyFromReader to return 0 copied rows, but got %d", copyCount)
	}

	ensureConnValid(t, conn)
}

func TestConnCopyFromGzipReader(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	mustExec(t, conn, `create temporary table foo(
		a int4,
		b varchar
	)`)

	f, err := ioutil.TempFile("", "*")
	if err != nil {
		t.Fatalf("Unexpected error for ioutil.TempFile: %v", err)
	}

	gw := gzip.NewWriter(f)

	inputRows := [][]interface{}{}
	for i := 0; i < 1000; i++ {
		val := strconv.Itoa(i * i)
		inputRows = append(inputRows, []interface{}{int32(i), val})
		_, err = gw.Write([]byte(fmt.Sprintf("%d,\"%s\"\n", i, val)))
		if err != nil {
			t.Errorf("Unexpected error for gw.Write: %v", err)
		}
	}

	err = gw.Close()
	if err != nil {
		t.Fatalf("Unexpected error for gw.Close: %v", err)
	}

	_, err = f.Seek(0, 0)
	if err != nil {
		t.Fatalf("Unexpected error for f.Seek: %v", err)
	}

	gr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatalf("Unexpected error for gzip.NewReader: %v", err)
	}

	res, err := conn.CopyFromReader(gr, "COPY foo FROM STDIN WITH (FORMAT csv)")
	if err != nil {
		t.Errorf("Unexpected error for CopyFromReader: %v", err)
	}

	copyCount := int(res.RowsAffected())
	if copyCount != len(inputRows) {
		t.Errorf("Expected CopyFromReader to return 1000 copied rows, but got %d", copyCount)
	}

	err = gr.Close()
	if err != nil {
		t.Errorf("Unexpected error for gr.Close: %v", err)
	}

	err = f.Close()
	if err != nil {
		t.Errorf("Unexpected error for f.Close: %v", err)
	}

	err = os.Remove(f.Name())
	if err != nil {
		t.Errorf("Unexpected error for os.Remove: %v", err)
	}

	rows, err := conn.Query("select * from foo")
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
