package pgx_test

import (
	"bytes"
	"fmt"
	"github.com/jackc/pgx"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestConnect(t *testing.T) {
	t.Parallel()

	conn, err := pgx.Connect(*defaultConnConfig)
	if err != nil {
		t.Fatalf("Unable to establish connection: %v", err)
	}

	if _, present := conn.RuntimeParams["server_version"]; !present {
		t.Error("Runtime parameters not stored")
	}

	if conn.Pid == 0 {
		t.Error("Backend PID not stored")
	}

	if conn.SecretKey == 0 {
		t.Error("Backend secret key not stored")
	}

	var currentDB string
	err = conn.QueryRow("select current_database()").Scan(&currentDB)
	if err != nil {
		t.Fatalf("QueryRow Scan unexpectedly failed: %v", err)
	}
	if currentDB != defaultConnConfig.Database {
		t.Errorf("Did not connect to specified database (%v)", defaultConnConfig.Database)
	}

	var user string
	err = conn.QueryRow("select current_user").Scan(&user)
	if err != nil {
		t.Fatalf("QueryRow Scan unexpectedly failed: %v", err)
	}
	if user != defaultConnConfig.User {
		t.Errorf("Did not connect as specified user (%v)", defaultConnConfig.User)
	}

	err = conn.Close()
	if err != nil {
		t.Fatal("Unable to close connection")
	}
}

func TestConnectWithUnixSocketDirectory(t *testing.T) {
	t.Parallel()

	// /.s.PGSQL.5432
	if unixSocketConnConfig == nil {
		return
	}

	conn, err := pgx.Connect(*unixSocketConnConfig)
	if err != nil {
		t.Fatalf("Unable to establish connection: %v", err)
	}

	err = conn.Close()
	if err != nil {
		t.Fatal("Unable to close connection")
	}
}

func TestConnectWithUnixSocketFile(t *testing.T) {
	t.Parallel()

	if unixSocketConnConfig == nil {
		return
	}

	connParams := *unixSocketConnConfig
	connParams.Host = connParams.Host + "/.s.PGSQL.5432"
	conn, err := pgx.Connect(connParams)
	if err != nil {
		t.Fatalf("Unable to establish connection: %v", err)
	}

	err = conn.Close()
	if err != nil {
		t.Fatal("Unable to close connection")
	}
}

func TestConnectWithTcp(t *testing.T) {
	t.Parallel()

	if tcpConnConfig == nil {
		return
	}

	conn, err := pgx.Connect(*tcpConnConfig)
	if err != nil {
		t.Fatal("Unable to establish connection: " + err.Error())
	}

	err = conn.Close()
	if err != nil {
		t.Fatal("Unable to close connection")
	}
}

func TestConnectWithTLS(t *testing.T) {
	t.Parallel()

	if tlsConnConfig == nil {
		return
	}

	conn, err := pgx.Connect(*tlsConnConfig)
	if err != nil {
		t.Fatal("Unable to establish connection: " + err.Error())
	}

	err = conn.Close()
	if err != nil {
		t.Fatal("Unable to close connection")
	}
}

func TestConnectWithInvalidUser(t *testing.T) {
	t.Parallel()

	if invalidUserConnConfig == nil {
		return
	}

	_, err := pgx.Connect(*invalidUserConnConfig)
	pgErr, ok := err.(pgx.PgError)
	if !ok {
		t.Fatalf("Expected to receive a PgError with code 28000, instead received: %v", err)
	}
	if pgErr.Code != "28000" && pgErr.Code != "28P01" {
		t.Fatalf("Expected to receive a PgError with code 28000 or 28P01, instead received: %v", pgErr)
	}
}

func TestConnectWithPlainTextPassword(t *testing.T) {
	t.Parallel()

	if plainPasswordConnConfig == nil {
		return
	}

	conn, err := pgx.Connect(*plainPasswordConnConfig)
	if err != nil {
		t.Fatal("Unable to establish connection: " + err.Error())
	}

	err = conn.Close()
	if err != nil {
		t.Fatal("Unable to close connection")
	}
}

func TestConnectWithMD5Password(t *testing.T) {
	t.Parallel()

	if md5ConnConfig == nil {
		return
	}

	conn, err := pgx.Connect(*md5ConnConfig)
	if err != nil {
		t.Fatal("Unable to establish connection: " + err.Error())
	}

	err = conn.Close()
	if err != nil {
		t.Fatal("Unable to close connection")
	}
}

func TestConnectWithConnectionRefused(t *testing.T) {
	t.Parallel()

	// Presumably nothing is listening on 127.0.0.1:1
	bad := *defaultConnConfig
	bad.Host = "127.0.0.1"
	bad.Port = 1

	_, err := pgx.Connect(bad)
	if !strings.Contains(err.Error(), "connection refused") {
		t.Fatal("Unable to establish connection: " + err.Error())
	}
}

func TestParseURI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		url        string
		connParams pgx.ConnConfig
	}{
		{
			url: "postgres://jack:secret@localhost:5432/mydb",
			connParams: pgx.ConnConfig{
				User:     "jack",
				Password: "secret",
				Host:     "localhost",
				Port:     5432,
				Database: "mydb",
			},
		},
		{
			url: "postgresql://jack:secret@localhost:5432/mydb",
			connParams: pgx.ConnConfig{
				User:     "jack",
				Password: "secret",
				Host:     "localhost",
				Port:     5432,
				Database: "mydb",
			},
		},
		{
			url: "postgres://jack@localhost:5432/mydb",
			connParams: pgx.ConnConfig{
				User:     "jack",
				Host:     "localhost",
				Port:     5432,
				Database: "mydb",
			},
		},
		{
			url: "postgres://jack@localhost/mydb",
			connParams: pgx.ConnConfig{
				User:     "jack",
				Host:     "localhost",
				Database: "mydb",
			},
		},
	}

	for i, tt := range tests {
		connParams, err := pgx.ParseURI(tt.url)
		if err != nil {
			t.Errorf("%d. Unexpected error from pgx.ParseURL(%q) => %v", i, tt.url, err)
			continue
		}

		if connParams != tt.connParams {
			t.Errorf("%d. expected %#v got %#v", i, tt.connParams, connParams)
		}
	}
}

func TestExec(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	if results := mustExec(t, conn, "create temporary table foo(id integer primary key);"); results != "CREATE TABLE" {
		t.Error("Unexpected results from Exec")
	}

	// Accept parameters
	if results := mustExec(t, conn, "insert into foo(id) values($1)", 1); results != "INSERT 0 1" {
		t.Errorf("Unexpected results from Exec: %v", results)
	}

	if results := mustExec(t, conn, "drop table foo;"); results != "DROP TABLE" {
		t.Error("Unexpected results from Exec")
	}

	// Multiple statements can be executed -- last command tag is returned
	if results := mustExec(t, conn, "create temporary table foo(id serial primary key); drop table foo;"); results != "DROP TABLE" {
		t.Error("Unexpected results from Exec")
	}

	// Can execute longer SQL strings than sharedBufferSize
	if results := mustExec(t, conn, strings.Repeat("select 42; ", 1000)); results != "SELECT 1" {
		t.Errorf("Unexpected results from Exec: %v", results)
	}
}

func TestExecFailure(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	if _, err := conn.Exec("select;"); err == nil {
		t.Fatal("Expected SQL syntax error")
	}

	qr, _ := conn.Query("select 1")
	qr.Close()
	if qr.Err() != nil {
		t.Fatalf("Exec failure appears to have broken connection: %v", qr.Err())
	}
}

func TestConnQuery(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	var sum, rowCount int32

	rows, err := conn.Query("select generate_series(1,$1)", 10)
	if err != nil {
		t.Fatalf("conn.Query failed: ", err)
	}
	defer rows.Close()

	for rows.NextRow() {
		var n int32
		rows.Scan(&n)
		sum += n
		rowCount++
	}

	if rows.Err() != nil {
		t.Fatalf("conn.Query failed: ", err)
	}

	if rowCount != 10 {
		t.Error("Select called onDataRow wrong number of times")
	}
	if sum != 55 {
		t.Error("Wrong values returned")
	}
}

// Do a simple query to ensure the connection is still usable
func ensureConnValid(t *testing.T, conn *pgx.Conn) {
	var sum, rowCount int32

	qr, err := conn.Query("select generate_series(1,$1)", 10)
	if err != nil {
		t.Fatalf("conn.Query failed: ", err)
	}
	defer qr.Close()

	for qr.NextRow() {
		var n int32
		qr.Scan(&n)
		sum += n
		rowCount++
	}

	if qr.Err() != nil {
		t.Fatalf("conn.Query failed: ", err)
	}

	if rowCount != 10 {
		t.Error("Select called onDataRow wrong number of times")
	}
	if sum != 55 {
		t.Error("Wrong values returned")
	}
}

// Test that a connection stays valid when query results are closed early
func TestConnQueryCloseEarly(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	// Immediately close query without reading any rows
	qr, err := conn.Query("select generate_series(1,$1)", 10)
	if err != nil {
		t.Fatalf("conn.Query failed: ", err)
	}
	qr.Close()

	ensureConnValid(t, conn)

	// Read partial response then close
	qr, err = conn.Query("select generate_series(1,$1)", 10)
	if err != nil {
		t.Fatalf("conn.Query failed: ", err)
	}

	ok := qr.NextRow()
	if !ok {
		t.Fatal("qr.NextRow terminated early")
	}

	var n int32
	qr.Scan(&n)
	if n != 1 {
		t.Fatalf("Expected 1 from first row, but got %v", n)
	}

	qr.Close()

	ensureConnValid(t, conn)
}

// Test that a connection stays valid when query results read incorrectly
func TestConnQueryReadWrongTypeError(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	// Read a single value incorrectly
	qr, err := conn.Query("select generate_series(1,$1)", 10)
	if err != nil {
		t.Fatalf("conn.Query failed: ", err)
	}

	rowsRead := 0

	for qr.NextRow() {
		var t time.Time
		qr.Scan(&t)
		rowsRead++
	}

	if rowsRead != 1 {
		t.Fatalf("Expected error to cause only 1 row to be read, but %d were read", rowsRead)
	}

	if qr.Err() == nil {
		t.Fatal("Expected QueryResult to have an error after an improper read but it didn't")
	}

	ensureConnValid(t, conn)
}

// Test that a connection stays valid when query results read incorrectly
func TestConnQueryReadTooManyValues(t *testing.T) {
	// t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	// Read too many values
	qr, err := conn.Query("select generate_series(1,$1)", 10)
	if err != nil {
		t.Fatalf("conn.Query failed: ", err)
	}

	rowsRead := 0

	for qr.NextRow() {
		var n, m int32
		qr.Scan(&n, &m)
		rowsRead++
	}

	if rowsRead != 1 {
		t.Fatalf("Expected error to cause only 1 row to be read, but %d were read", rowsRead)
	}

	if qr.Err() == nil {
		t.Fatal("Expected QueryResult to have an error after an improper read but it didn't")
	}

	ensureConnValid(t, conn)
}

func TestConnQueryUnpreparedScanner(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	qr, err := conn.Query("select null::int8, 1::int8")
	if err != nil {
		t.Fatalf("conn.Query failed: ", err)
	}

	ok := qr.NextRow()
	if !ok {
		t.Fatal("qr.NextRow terminated early")
	}

	var n, m pgx.NullInt64
	err = qr.Scan(&n, &m)
	if err != nil {
		t.Fatalf("qr.Scan failed: ", err)
	}
	qr.Close()

	if n.Valid {
		t.Error("Null should not be valid, but it was")
	}

	if !m.Valid {
		t.Error("1 should be valid, but it wasn't")
	}

	if m.Int64 != 1 {
		t.Errorf("m.Int64 should have been 1, but it was %v", m.Int64)
	}

	ensureConnValid(t, conn)
}

func TestConnQueryPreparedScanner(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	mustPrepare(t, conn, "scannerTest", "select null::int8, 1::int8")

	qr, err := conn.Query("scannerTest")
	if err != nil {
		t.Fatalf("conn.Query failed: ", err)
	}

	ok := qr.NextRow()
	if !ok {
		t.Fatal("qr.NextRow terminated early")
	}

	var n, m pgx.NullInt64
	err = qr.Scan(&n, &m)
	if err != nil {
		t.Fatalf("qr.Scan failed: ", err)
	}
	qr.Close()

	if n.Valid {
		t.Error("Null should not be valid, but it was")
	}

	if !m.Valid {
		t.Error("1 should be valid, but it wasn't")
	}

	if m.Int64 != 1 {
		t.Errorf("m.Int64 should have been 1, but it was %v", m.Int64)
	}

	ensureConnValid(t, conn)
}

func TestConnQueryUnpreparedEncoder(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	n := pgx.NullInt64{Int64: 1, Valid: true}

	qr, err := conn.Query("select $1::int8", &n)
	if err != nil {
		t.Fatalf("conn.Query failed: ", err)
	}

	ok := qr.NextRow()
	if !ok {
		t.Fatal("qr.NextRow terminated early")
	}

	var m pgx.NullInt64
	err = qr.Scan(&m)
	if err != nil {
		t.Fatalf("qr.Scan failed: ", err)
	}
	qr.Close()

	if !m.Valid {
		t.Error("m should be valid, but it wasn't")
	}

	if m.Int64 != 1 {
		t.Errorf("m.Int64 should have been 1, but it was %v", m.Int64)
	}

	ensureConnValid(t, conn)
}

func TestPrepare(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	_, err := conn.Prepare("test", "select $1::varchar")
	if err != nil {
		t.Errorf("Unable to prepare statement: %v", err)
		return
	}

	var s string
	err = conn.QueryRow("test", "hello").Scan(&s)
	if err != nil {
		t.Errorf("Executing prepared statement failed: %v", err)
	}

	if s != "hello" {
		t.Errorf("Prepared statement did not return expected value: %v", s)
	}
}

func TestPrepareFailure(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	if _, err := conn.Prepare("badSQL", "select foo"); err == nil {
		t.Fatal("Prepare should have failed with syntax error")
	}

	ensureConnValid(t, conn)
}

func TestTransaction(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	createSql := `
		create temporary table foo(
			id integer,
			unique (id) initially deferred
		);
	`

	if _, err := conn.Exec(createSql); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Transaction happy path -- it executes function and commits
	committed, err := conn.Transaction(func() bool {
		mustExec(t, conn, "insert into foo(id) values (1)")
		return true
	})
	if err != nil {
		t.Fatalf("Transaction unexpectedly failed: %v", err)
	}
	if !committed {
		t.Fatal("Transaction was not committed")
	}

	var n int64
	err = conn.QueryRow("select count(*) from foo").Scan(&n)
	if err != nil {
		t.Fatalf("QueryRow Scan failed: %v", err)
	}
	if n != 1 {
		t.Fatalf("Did not receive correct number of rows: %v", n)
	}

	mustExec(t, conn, "truncate foo")

	// It rolls back when passed function returns false
	committed, err = conn.Transaction(func() bool {
		mustExec(t, conn, "insert into foo(id) values (1)")
		return false
	})
	if err != nil {
		t.Fatalf("Transaction unexpectedly failed: %v", err)
	}
	if committed {
		t.Fatal("Transaction should not have been committed")
	}
	err = conn.QueryRow("select count(*) from foo").Scan(&n)
	if err != nil {
		t.Fatalf("QueryRow Scan failed: %v", err)
	}
	if n != 0 {
		t.Fatalf("Did not receive correct number of rows: %v", n)
	}

	// it rolls back changes when connection is in error state
	committed, err = conn.Transaction(func() bool {
		mustExec(t, conn, "insert into foo(id) values (1)")
		if _, err := conn.Exec("invalid"); err == nil {
			t.Fatal("Exec was supposed to error but didn't")
		}
		return true
	})
	if err != nil {
		t.Fatalf("Transaction unexpectedly failed: %v", err)
	}
	if committed {
		t.Fatal("Transaction was committed when it shouldn't have been")
	}
	err = conn.QueryRow("select count(*) from foo").Scan(&n)
	if err != nil {
		t.Fatalf("QueryRow Scan failed: %v", err)
	}
	if n != 0 {
		t.Fatalf("Did not receive correct number of rows: %v", n)
	}

	// when commit fails
	committed, err = conn.Transaction(func() bool {
		mustExec(t, conn, "insert into foo(id) values (1)")
		mustExec(t, conn, "insert into foo(id) values (1)")
		return true
	})
	if err == nil {
		t.Fatal("Transaction should have failed but didn't")
	}
	if committed {
		t.Fatal("Transaction was committed when it should have failed")
	}

	err = conn.QueryRow("select count(*) from foo").Scan(&n)
	if err != nil {
		t.Fatalf("QueryRow Scan failed: %v", err)
	}
	if n != 0 {
		t.Fatalf("Did not receive correct number of rows: %v", n)
	}

	// when something in transaction panics
	func() {
		defer func() {
			recover()
		}()

		committed, err = conn.Transaction(func() bool {
			mustExec(t, conn, "insert into foo(id) values (1)")
			panic("stop!")
		})

		err = conn.QueryRow("select count(*) from foo").Scan(&n)
		if err != nil {
			t.Fatalf("QueryRow Scan failed: %v", err)
		}
		if n != 0 {
			t.Fatalf("Did not receive correct number of rows: %v", n)
		}
	}()
}

func TestTransactionIso(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	isoLevels := []string{pgx.Serializable, pgx.RepeatableRead, pgx.ReadCommitted, pgx.ReadUncommitted}
	for _, iso := range isoLevels {
		_, err := conn.TransactionIso(iso, func() bool {
			var level string
			conn.QueryRow("select current_setting('transaction_isolation')").Scan(&level)
			if level != iso {
				t.Errorf("Expected to be in isolation level %v but was %v", iso, level)
			}
			return true
		})
		if err != nil {
			t.Fatalf("Unexpected transaction failure: %v", err)
		}
	}
}

func TestListenNotify(t *testing.T) {
	t.Parallel()

	listener := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, listener)

	if err := listener.Listen("chat"); err != nil {
		t.Fatalf("Unable to start listening: %v", err)
	}

	notifier := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, notifier)

	mustExec(t, notifier, "notify chat")

	// when notification is waiting on the socket to be read
	notification, err := listener.WaitForNotification(time.Second)
	if err != nil {
		t.Fatalf("Unexpected error on WaitForNotification: %v", err)
	}
	if notification.Channel != "chat" {
		t.Errorf("Did not receive notification on expected channel: %v", notification.Channel)
	}

	// when notification has already been read during previous query
	mustExec(t, notifier, "notify chat")
	qr, _ := listener.Query("select 1")
	qr.Close()
	if qr.Err() != nil {
		t.Fatalf("Unexpected error on Query: %v", qr.Err())
	}
	notification, err = listener.WaitForNotification(0)
	if err != nil {
		t.Fatalf("Unexpected error on WaitForNotification: %v", err)
	}
	if notification.Channel != "chat" {
		t.Errorf("Did not receive notification on expected channel: %v", notification.Channel)
	}

	// when timeout occurs
	notification, err = listener.WaitForNotification(time.Millisecond)
	if err != pgx.ErrNotificationTimeout {
		t.Errorf("WaitForNotification returned the wrong kind of error: %v", err)
	}
	if notification != nil {
		t.Errorf("WaitForNotification returned an unexpected notification: %v", notification)
	}

	// listener can listen again after a timeout
	mustExec(t, notifier, "notify chat")
	notification, err = listener.WaitForNotification(time.Second)
	if err != nil {
		t.Fatalf("Unexpected error on WaitForNotification: %v", err)
	}
	if notification.Channel != "chat" {
		t.Errorf("Did not receive notification on expected channel: %v", notification.Channel)
	}
}

func TestFatalRxError(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		var n int32
		var s string
		err := conn.QueryRow("select 1::int4, pg_sleep(10)::varchar").Scan(&n, &s)
		if err, ok := err.(pgx.PgError); !ok || err.Severity != "FATAL" {
			t.Fatalf("Expected QueryRow Scan to return fatal PgError, but instead received %v", err)
		}
	}()

	otherConn, err := pgx.Connect(*defaultConnConfig)
	if err != nil {
		t.Fatalf("Unable to establish connection: %v", err)
	}
	defer otherConn.Close()

	if _, err := otherConn.Exec("select pg_terminate_backend($1)", conn.Pid); err != nil {
		t.Fatalf("Unable to kill backend PostgreSQL process: %v", err)
	}

	wg.Wait()

	if conn.IsAlive() {
		t.Fatal("Connection should not be live but was")
	}
}

func TestFatalTxError(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	otherConn, err := pgx.Connect(*defaultConnConfig)
	if err != nil {
		t.Fatalf("Unable to establish connection: %v", err)
	}
	defer otherConn.Close()

	_, err = otherConn.Exec("select pg_terminate_backend($1)", conn.Pid)
	if err != nil {
		t.Fatalf("Unable to kill backend PostgreSQL process: %v", err)
	}

	_, err = conn.Query("select 1")
	if err == nil {
		t.Fatal("Expected error but none occurred")
	}

	if conn.IsAlive() {
		t.Fatal("Connection should not be live but was")
	}
}

func TestCommandTag(t *testing.T) {
	t.Parallel()

	var tests = []struct {
		commandTag   pgx.CommandTag
		rowsAffected int64
	}{
		{commandTag: "INSERT 0 5", rowsAffected: 5},
		{commandTag: "UPDATE 0", rowsAffected: 0},
		{commandTag: "UPDATE 1", rowsAffected: 1},
		{commandTag: "DELETE 0", rowsAffected: 0},
		{commandTag: "DELETE 1", rowsAffected: 1},
		{commandTag: "CREATE TABLE", rowsAffected: 0},
		{commandTag: "ALTER TABLE", rowsAffected: 0},
		{commandTag: "DROP TABLE", rowsAffected: 0},
	}

	for i, tt := range tests {
		actual := tt.commandTag.RowsAffected()
		if tt.rowsAffected != actual {
			t.Errorf(`%d. "%s" should have affected %d rows but it was %d`, i, tt.commandTag, tt.rowsAffected, actual)
		}
	}
}

func TestQueryRowCoreTypes(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	type allTypes struct {
		s   string
		i16 int16
		i32 int32
		i64 int64
		f32 float32
		f64 float64
		b   bool
	}

	var actual, zero allTypes

	tests := []struct {
		sql       string
		queryArgs []interface{}
		scanArgs  []interface{}
		expected  allTypes
	}{
		{"select $1::text", []interface{}{"Jack"}, []interface{}{&actual.s}, allTypes{s: "Jack"}},
		{"select $1::int2", []interface{}{int16(42)}, []interface{}{&actual.i16}, allTypes{i16: 42}},
		{"select $1::int4", []interface{}{int32(42)}, []interface{}{&actual.i32}, allTypes{i32: 42}},
		{"select $1::int8", []interface{}{int64(42)}, []interface{}{&actual.i64}, allTypes{i64: 42}},
		{"select $1::float4", []interface{}{float32(1.23)}, []interface{}{&actual.f32}, allTypes{f32: 1.23}},
		{"select $1::float8", []interface{}{float64(1.23)}, []interface{}{&actual.f64}, allTypes{f64: 1.23}},
		{"select $1::bool", []interface{}{true}, []interface{}{&actual.b}, allTypes{b: true}},
	}

	for i, tt := range tests {
		psName := fmt.Sprintf("success%d", i)
		mustPrepare(t, conn, psName, tt.sql)

		for _, sql := range []string{tt.sql, psName} {
			actual = zero

			err := conn.QueryRow(sql, tt.queryArgs...).Scan(tt.scanArgs...)
			if err != nil {
				t.Errorf("%d. Unexpected failure: %v (sql -> %v, queryArgs -> %v)", i, err, sql, tt.queryArgs)
			}

			if actual != tt.expected {
				t.Errorf("%d. Expected %v, got %v (sql -> %v, queryArgs -> %v)", i, tt.expected, actual, sql, tt.queryArgs)
			}

			ensureConnValid(t, conn)
		}
	}
}

func TestQueryRowCoreBytea(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	var actual []byte
	sql := "select $1::bytea"
	queryArg := []byte{0, 15, 255, 17}
	expected := []byte{0, 15, 255, 17}

	psName := "selectBytea"
	mustPrepare(t, conn, psName, sql)

	for _, sql := range []string{sql, psName} {
		actual = nil

		err := conn.QueryRow(sql, queryArg).Scan(&actual)
		if err != nil {
			t.Errorf("Unexpected failure: %v (sql -> %v)", err, sql)
		}

		if bytes.Compare(actual, expected) != 0 {
			t.Errorf("Expected %v, got %v (sql -> %v)", expected, actual, sql)
		}

		ensureConnValid(t, conn)
	}
}

func TestQueryRowUnpreparedErrors(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	type allTypes struct {
		s   string
		i16 int16
		i32 int32
		i64 int64
		f32 float32
		f64 float64
		b   bool
	}

	var actual, zero allTypes

	tests := []struct {
		sql       string
		queryArgs []interface{}
		scanArgs  []interface{}
		err       string
	}{
		{"select $1", []interface{}{"Jack"}, []interface{}{&actual.i16}, "Expected type oid 21 but received type oid 705"},
		{"select $1::badtype", []interface{}{"Jack"}, []interface{}{&actual.i16}, `type "badtype" does not exist`},
		{"SYNTAX ERROR", []interface{}{}, []interface{}{&actual.i16}, "SQLSTATE 42601"},
	}

	for i, tt := range tests {
		actual = zero

		err := conn.QueryRow(tt.sql, tt.queryArgs...).Scan(tt.scanArgs...)
		if err == nil {
			t.Errorf("%d. Unexpected success (sql -> %v, queryArgs -> %v)", i, tt.sql, tt.queryArgs)
		}
		if !strings.Contains(err.Error(), tt.err) {
			t.Errorf("%d. Expected error to contain %s, but got %v (sql -> %v, queryArgs -> %v)", i, tt.err, err, tt.sql, tt.queryArgs)
		}

		ensureConnValid(t, conn)
	}
}

func TestQueryRowPreparedErrors(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	type allTypes struct {
		s   string
		i16 int16
		i32 int32
		i64 int64
		f32 float32
		f64 float64
		b   bool
	}

	var actual, zero allTypes

	tests := []struct {
		sql       string
		queryArgs []interface{}
		scanArgs  []interface{}
		err       string
	}{
		{"select $1::text", []interface{}{"Jack"}, []interface{}{&actual.i16}, "Expected type oid 21 but received type oid 25"},
	}

	for i, tt := range tests {
		psName := fmt.Sprintf("ps%d", i)
		mustPrepare(t, conn, psName, tt.sql)

		actual = zero

		err := conn.QueryRow(psName, tt.queryArgs...).Scan(tt.scanArgs...)
		if err == nil {
			t.Errorf("%d. Unexpected success (sql -> %v, queryArgs -> %v)", i, tt.sql, tt.queryArgs)
		}
		if !strings.Contains(err.Error(), tt.err) {
			t.Errorf("%d. Expected error to contain %s, but got %v (sql -> %v, queryArgs -> %v)", i, tt.err, err, tt.sql, tt.queryArgs)
		}

		ensureConnValid(t, conn)
	}
}

func TestQueryRowNoResults(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	sql := "select 1 where 1=0"
	psName := "selectNothing"
	mustPrepare(t, conn, psName, sql)

	for _, sql := range []string{sql, psName} {
		var n int32
		err := conn.QueryRow(sql).Scan(&n)
		if err != pgx.ErrNoRows {
			t.Errorf("Expected pgx.ErrNoRows, got %v", err)
		}

		ensureConnValid(t, conn)
	}
}

func TestQueryPreparedEncodeError(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	mustPrepare(t, conn, "testTranscode", "select $1::integer")
	defer func() {
		if err := conn.Deallocate("testTranscode"); err != nil {
			t.Fatalf("Unable to deallocate prepared statement: %v", err)
		}
	}()

	_, err := conn.Query("testTranscode", "wrong")
	switch {
	case err == nil:
		t.Error("Expected transcode error to return error, but it didn't")
	case err.Error() == "Expected integer representable in int32, received string wrong":
		// Correct behavior
	default:
		t.Errorf("Expected transcode error, received %v", err)
	}
}
