package pgx_test

import (
	"bufio"
	"net"
	"reflect"
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

	inputRows := [][]interface{}{
		{int16(0), int32(1), int64(2), "abc", "efg", time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2010, 2, 3, 4, 5, 6, 0, time.Local)},
		{nil, nil, nil, nil, nil, nil, nil},
	}

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

	inputRows := [][]interface{}{}

	for i := 0; i < 10000; i++ {
		inputRows = append(inputRows, []interface{}{int16(0), int32(1), int64(2), "abc", "efg", time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2010, 2, 3, 4, 5, 6, 0, time.Local), []byte{111, 111, 111, 111}})
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

func TestConnCopyFromStringReader(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	mustExec(t, conn, `create temporary table foo(
		id serial,
		a int2,
		b int4,
		c int8,
		d varchar,
		e text,
		f jsonb,
		g date,
		h timestamptz,
		i bool
	)`)
	var sepComma rune = pgx.CommaSeparator
	var nullPlaceholder string = "![null]"

	inputData := `"12", 1536363341,36427536347546,"Rocky","Rocky vs the world","{"age": 30}","2010-11-23","2010-10-22 23:45:00 EST",f
22, 5736734 ,"7254745","Bullwinkle","A dubious existence","{"wizard":"oz"}",,"1994-10-22 23:45:00 GMT","true"
"32", ,36427536347546,"Garibaldi",'Unifier of Italy',,,"2010-10-22 23:45:00 PST","F"
"42",9483,453,![null],"Stranger than kindness","{"flower":"rose"}","1957-06-12",,"1"
"52","2619",285723,"","defeated, but not vanquished",,"1998-03-05","2016-10-22 10:45:00.157999592 CET",0
`
	var inputRowCount = 5

	inputRows := strings.NewReader(inputData)

	copySourceReader, err := pgx.CopyFromReader(inputRows, sepComma, &nullPlaceholder, "int2", "int4", "int8", "varchar", "text", "jsonb", "date", "timestamptz", "bool")
	if err != nil {
		t.Errorf("Unexpected error from CopyFromReader: %v", err)
	}
	copyCount, err := conn.CopyFrom(pgx.Identifier{"foo"}, []string{"a", "b", "c", "d", "e", "f", "g", "h", "i"}, copySourceReader)
	if err != nil {
		t.Errorf("Unexpected error for CopyFrom: %v", err)
	}
	if copyCount != inputRowCount {
		t.Errorf("Expected CopyFrom to return %d copied rows, but got %d", inputRowCount, copyCount)
	}

	rows, err := conn.Query("select a,b,c,d,e,f,g,h,i from foo")
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

	if v, ok := (outputRows[0][0]).(int16); !ok {
		t.Errorf("Unexpected type for outputRows[0][0] - expected: %s", "int16")
	} else {
		if v != int16(12) {
			t.Errorf("Unexpected value for outputRows[0][0] - expected %s got %d", "12", v)
		}
	}

	if v, ok := (outputRows[1][5]).(map[string]interface{}); !ok {
		t.Errorf("Unexpected type for outputRows[1][5] - expected: %s for value %v", "map[string]interface{}", outputRows[1][5])
	} else {
		if v["wizard"] != "oz" {
			t.Errorf("Unexpected value for outputRows[1][5] - expected %s got %s", "{\"wizard\":\"oz\"}", v)
		}
	}

	ensureConnValid(t, conn)
}

func TestConnCopyFromNetworkReaderLarge(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	mustExec(t, conn, `create temporary table foo(
		id serial,
		a int2,
		b int4,
		c int8,
		d varchar,
		e text,
		f jsonb,
		g date,
		h timestamptz,
		i bool
	)`)
	var sepComma rune = pgx.CommaSeparator
	var nullPlaceholder string = "![null]"

	inputData := `"12", 1536363341,36427536347546,"Rocky","Rocky vs the world","{"age": 30}","2010-11-23","2010-10-22 23:45:00 EST",f
22, 5736734 ,"7254745","Bullwinkle","A dubious existence","{"wizard":"oz"}",,"1994-10-22 23:45:00 GMT","true"
"32", ,36427536347546,"Garibaldi",'Unifier of Italy',,,"2010-10-22 23:45:00 PST","F"
"42",9483,453,![null],"Stranger than kindness","{"flower":"rose"}","1957-06-12",,"1"
"52","2619",285723,"","defeated, but not vanquished",,"1998-03-05","2016-10-22 10:45:00.157999592 CET",0
`
	var inputRowCount = 5
	var repetitions = 20000

	chanStatus := make(chan string, 6)
	serverAddressAndPort := "127.0.0.1:8368"

	// Start the writer (server)
	go func(data string, reps int, statusChan chan string) {

		ln, err := net.Listen("tcp", serverAddressAndPort)
		if err != nil {
			statusChan <- err.Error()
			return
		}
		statusChan <- "OK"

		conn, err := ln.Accept()
		if err != nil {
			statusChan <- err.Error()
			return
		}

		for i := 1; i <= reps; i++ {
			_, err = conn.Write([]byte(inputData))
			if err != nil {
				statusChan <- err.Error()
				return
			}
		}

		if err := conn.Close(); err != nil {
			statusChan <- err.Error()
			return
		}

	}(inputData, repetitions, chanStatus)

	// Wait for the server to initialize or time out
	select {
	case status := <-chanStatus:
		if status != "OK" {
			t.Errorf("Unexpected server error: %s", status)
		}
	case <-time.After(time.Second * 3):
		t.Errorf("Timed out waiting for the server to initialize")
	}

	// Initialize the network client
	netConn, err := net.Dial("tcp", serverAddressAndPort)
	if err != nil {
		t.Errorf("Unexpected client dial error: %v", err)
	}
	defer netConn.Close()
	netReader := bufio.NewReader(netConn)
	copySourceReader, err := pgx.CopyFromReader(netReader, sepComma, &nullPlaceholder, "int2", "int4", "int8", "varchar", "text", "jsonb", "date", "timestamptz", "bool")
	if err != nil {
		t.Errorf("Unexpected error from CopyFromReader: %v", err)
	}

	copyCount, err := conn.CopyFrom(pgx.Identifier{"foo"}, []string{"a", "b", "c", "d", "e", "f", "g", "h", "i"}, copySourceReader)
	if err != nil {
		t.Errorf("Unexpected error for CopyFrom: %v", err)
	}
	if copyCount != inputRowCount*repetitions {
		t.Errorf("Expected CopyFrom to return %d copied rows, but got %d", inputRowCount, copyCount)
	}

	// Based on how many rows were inserted, select 5 rows at "random" with
	// an offset that is a multiple of 5, so we can do value comparison.
	// (change the 5000 offset as needed).
	rows, err := conn.Query("select a,b,c,d,e,f,g,h,i from foo order by id limit 5 offset 5000")
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

	if v, ok := (outputRows[0][0]).(int16); !ok {
		t.Errorf("Unexpected type for outputRows[0][0] - expected: %s", "int16")
	} else {
		if v != int16(12) {
			t.Errorf("Unexpected value for outputRows[0][0] - expected %s got %d", "12", v)
		}
	}

	if v, ok := (outputRows[1][5]).(map[string]interface{}); !ok {
		t.Errorf("Unexpected type for outputRows[1][5] - expected: %s for value %v", "map[string]interface{}", outputRows[1][5])
	} else {
		if v["wizard"] != "oz" {
			t.Errorf("Unexpected value for outputRows[1][5] - expected %s got %s", "{\"wizard\":\"oz\"}", v)
		}
	}

	ensureConnValid(t, conn)
}
