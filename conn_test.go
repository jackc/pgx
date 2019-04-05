package pgx_test

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"
	"github.com/stretchr/testify/require"
)

func TestCrateDBConnect(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("PGX_TEST_CRATEDB_CONN_STRING")
	if connString == "" {
		t.Skipf("Skipping due to missing environment variable %v", "PGX_TEST_CRATEDB_CONN_STRING")
	}

	conn, err := pgx.Connect(context.Background(), connString)
	require.Nil(t, err)
	defer closeConn(t, conn)

	var result int
	err = conn.QueryRow("select 1 +1").Scan(&result)
	if err != nil {
		t.Fatalf("QueryRow Scan unexpectedly failed: %v", err)
	}
	if result != 2 {
		t.Errorf("bad result: %d", result)
	}
}

func TestConnect(t *testing.T) {
	t.Parallel()

	config := mustParseConfig(t, os.Getenv("PGX_TEST_DATABASE"))

	conn, err := pgx.ConnectConfig(context.Background(), &config)
	if err != nil {
		t.Fatalf("Unable to establish connection: %v", err)
	}

	if conn.ParameterStatus("server_version") == "" {
		t.Error("Runtime parameters not stored")
	}

	if conn.PID() == 0 {
		t.Error("Backend PID not stored")
	}

	var currentDB string
	err = conn.QueryRow("select current_database()").Scan(&currentDB)
	if err != nil {
		t.Fatalf("QueryRow Scan unexpectedly failed: %v", err)
	}
	if currentDB != config.Config.Database {
		t.Errorf("Did not connect to specified database (%v)", config.Config.Database)
	}

	var user string
	err = conn.QueryRow("select current_user").Scan(&user)
	if err != nil {
		t.Fatalf("QueryRow Scan unexpectedly failed: %v", err)
	}
	if user != config.Config.User {
		t.Errorf("Did not connect as specified user (%v)", config.Config.User)
	}

	err = conn.Close()
	if err != nil {
		t.Fatal("Unable to close connection")
	}
}

func TestConnectWithPreferSimpleProtocol(t *testing.T) {
	t.Parallel()

	connConfig := mustParseConfig(t, os.Getenv("PGX_TEST_DATABASE"))
	connConfig.PreferSimpleProtocol = true

	conn := mustConnect(t, connConfig)
	defer closeConn(t, conn)

	// If simple protocol is used we should be able to correctly scan the result
	// into a pgtype.Text as the integer will have been encoded in text.

	var s pgtype.Text
	err := conn.QueryRow("select $1::int4", 42).Scan(&s)
	if err != nil {
		t.Fatal(err)
	}

	if s.Get() != "42" {
		t.Fatalf(`expected "42", got %v`, s)
	}

	ensureConnValid(t, conn)
}

func TestExec(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	if results := mustExec(t, conn, "create temporary table foo(id integer primary key);"); string(results) != "CREATE TABLE" {
		t.Error("Unexpected results from Exec")
	}

	// Accept parameters
	if results := mustExec(t, conn, "insert into foo(id) values($1)", 1); string(results) != "INSERT 0 1" {
		t.Errorf("Unexpected results from Exec: %v", results)
	}

	if results := mustExec(t, conn, "drop table foo;"); string(results) != "DROP TABLE" {
		t.Error("Unexpected results from Exec")
	}

	// Multiple statements can be executed -- last command tag is returned
	if results := mustExec(t, conn, "create temporary table foo(id serial primary key); drop table foo;"); string(results) != "DROP TABLE" {
		t.Error("Unexpected results from Exec")
	}

	// Can execute longer SQL strings than sharedBufferSize
	if results := mustExec(t, conn, strings.Repeat("select 42; ", 1000)); string(results) != "SELECT 1" {
		t.Errorf("Unexpected results from Exec: %v", results)
	}

	// Exec no-op which does not return a command tag
	if results := mustExec(t, conn, "--;"); string(results) != "" {
		t.Errorf("Unexpected results from Exec: %v", results)
	}
}

func TestExecFailure(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	if _, err := conn.Exec(context.Background(), "selct;"); err == nil {
		t.Fatal("Expected SQL syntax error")
	}
	if !conn.LastStmtSent() {
		t.Error("Expected LastStmtSent to return true")
	}

	rows, _ := conn.Query("select 1")
	rows.Close()
	if rows.Err() != nil {
		t.Fatalf("Exec failure appears to have broken connection: %v", rows.Err())
	}
	if !conn.LastStmtSent() {
		t.Error("Expected LastStmtSent to return true")
	}
}

func TestExecFailureWithArguments(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	if _, err := conn.Exec(context.Background(), "selct $1;", 1); err == nil {
		t.Fatal("Expected SQL syntax error")
	}
	if conn.LastStmtSent() {
		t.Error("Expected LastStmtSent to return false")
	}
}

func TestExecContextWithoutCancelation(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	commandTag, err := conn.Exec(ctx, "create temporary table foo(id integer primary key);")
	if err != nil {
		t.Fatal(err)
	}
	if string(commandTag) != "CREATE TABLE" {
		t.Fatalf("Unexpected results from ExecEx: %v", commandTag)
	}
	if !conn.LastStmtSent() {
		t.Error("Expected LastStmtSent to return true")
	}
}

func TestExecContextFailureWithoutCancelation(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	if _, err := conn.Exec(ctx, "selct;"); err == nil {
		t.Fatal("Expected SQL syntax error")
	}
	if !conn.LastStmtSent() {
		t.Error("Expected LastStmtSent to return true")
	}

	rows, _ := conn.Query("select 1")
	rows.Close()
	if rows.Err() != nil {
		t.Fatalf("ExecEx failure appears to have broken connection: %v", rows.Err())
	}
	if !conn.LastStmtSent() {
		t.Error("Expected LastStmtSent to return true")
	}
}

func TestExecContextFailureWithoutCancelationWithArguments(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	if _, err := conn.Exec(ctx, "selct $1;", 1); err == nil {
		t.Fatal("Expected SQL syntax error")
	}
	if conn.LastStmtSent() {
		t.Error("Expected LastStmtSent to return false")
	}
}

func TestExecFailureCloseBefore(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	closeConn(t, conn)

	if _, err := conn.Exec(context.Background(), "select 1"); err == nil {
		t.Fatal("Expected network error")
	}
	if conn.LastStmtSent() {
		t.Error("Expected LastStmtSent to return false")
	}
}

func TestExecExtendedProtocol(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	commandTag, err := conn.Exec(ctx, "create temporary table foo(name varchar primary key);")
	if err != nil {
		t.Fatal(err)
	}
	if string(commandTag) != "CREATE TABLE" {
		t.Fatalf("Unexpected results from Exec: %v", commandTag)
	}

	commandTag, err = conn.Exec(
		ctx,
		"insert into foo(name) values($1);",
		"bar",
	)
	if err != nil {
		t.Fatal(err)
	}
	if string(commandTag) != "INSERT 0 1" {
		t.Fatalf("Unexpected results from ExecEx: %v", commandTag)
	}

	ensureConnValid(t, conn)
}

func TestExecSimpleProtocol(t *testing.T) {
	t.Skip("TODO when with simple protocol supported in connection")
	// t.Parallel()

	// conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	// defer closeConn(t, conn)

	// ctx, cancelFunc := context.WithCancel(context.Background())
	// defer cancelFunc()

	// commandTag, err := conn.ExecEx(ctx, "create temporary table foo(name varchar primary key);", nil)
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// if string(commandTag) != "CREATE TABLE" {
	// 	t.Fatalf("Unexpected results from ExecEx: %v", commandTag)
	// }
	// if !conn.LastStmtSent() {
	// 	t.Error("Expected LastStmtSent to return true")
	// }

	// commandTag, err = conn.ExecEx(
	// 	ctx,
	// 	"insert into foo(name) values($1);",
	// 	&pgx.QueryExOptions{SimpleProtocol: true},
	// 	"bar'; drop table foo;--",
	// )
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// if string(commandTag) != "INSERT 0 1" {
	// 	t.Fatalf("Unexpected results from ExecEx: %v", commandTag)
	// }
	// if !conn.LastStmtSent() {
	// 	t.Error("Expected LastStmtSent to return true")
	// }
}

func TestExecExFailureCloseBefore(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	closeConn(t, conn)

	if _, err := conn.Exec(context.Background(), "select 1", nil); err == nil {
		t.Fatal("Expected network error")
	}
	if conn.LastStmtSent() {
		t.Error("Expected LastStmtSent to return false")
	}
}

func TestPrepare(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
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

	err = conn.Deallocate("test")
	if err != nil {
		t.Errorf("conn.Deallocate failed: %v", err)
	}

	// Create another prepared statement to ensure Deallocate left the connection
	// in a working state and that we can reuse the prepared statement name.

	_, err = conn.Prepare("test", "select $1::integer")
	if err != nil {
		t.Errorf("Unable to prepare statement: %v", err)
		return
	}

	var n int32
	err = conn.QueryRow("test", int32(1)).Scan(&n)
	if err != nil {
		t.Errorf("Executing prepared statement failed: %v", err)
	}

	if n != 1 {
		t.Errorf("Prepared statement did not return expected value: %v", s)
	}

	err = conn.Deallocate("test")
	if err != nil {
		t.Errorf("conn.Deallocate failed: %v", err)
	}
}

func TestPrepareBadSQLFailure(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	if _, err := conn.Prepare("badSQL", "select foo"); err == nil {
		t.Fatal("Prepare should have failed with syntax error")
	}

	ensureConnValid(t, conn)
}

func TestPrepareQueryManyParameters(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	tests := []struct {
		count   int
		succeed bool
	}{
		{
			count:   65534,
			succeed: true,
		},
		{
			count:   65535,
			succeed: true,
		},
		{
			count:   65536,
			succeed: false,
		},
		{
			count:   65537,
			succeed: false,
		},
	}

	for i, tt := range tests {
		params := make([]string, 0, tt.count)
		args := make([]interface{}, 0, tt.count)
		for j := 0; j < tt.count; j++ {
			params = append(params, fmt.Sprintf("($%d::text)", j+1))
			args = append(args, strconv.Itoa(j))
		}

		sql := "values" + strings.Join(params, ", ")

		psName := fmt.Sprintf("manyParams%d", i)
		_, err := conn.Prepare(psName, sql)
		if err != nil {
			if tt.succeed {
				t.Errorf("%d. %v", i, err)
			}
			continue
		}
		if !tt.succeed {
			t.Errorf("%d. Expected error but succeeded", i)
			continue
		}

		rows, err := conn.Query(psName, args...)
		if err != nil {
			t.Errorf("conn.Query failed: %v", err)
			continue
		}

		for rows.Next() {
			var s string
			rows.Scan(&s)
		}

		if rows.Err() != nil {
			t.Errorf("Reading query result failed: %v", err)
		}
	}

	ensureConnValid(t, conn)
}

func TestPrepareIdempotency(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	for i := 0; i < 2; i++ {
		_, err := conn.Prepare("test", "select 42::integer")
		if err != nil {
			t.Fatalf("%d. Unable to prepare statement: %v", i, err)
		}

		var n int32
		err = conn.QueryRow("test").Scan(&n)
		if err != nil {
			t.Errorf("%d. Executing prepared statement failed: %v", i, err)
		}

		if n != int32(42) {
			t.Errorf("%d. Prepared statement did not return expected value: %v", i, n)
		}
	}

	_, err := conn.Prepare("test", "select 'fail'::varchar")
	if err == nil {
		t.Fatalf("Prepare statement with same name but different SQL should have failed but it didn't")
		return
	}
}

func TestPrepareEx(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	_, err := conn.PrepareEx(context.Background(), "test", "select $1", &pgx.PrepareExOptions{ParameterOIDs: []pgtype.OID{pgtype.TextOID}})
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

	err = conn.Deallocate("test")
	if err != nil {
		t.Errorf("conn.Deallocate failed: %v", err)
	}
}

func TestFatalRxError(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		var n int32
		var s string
		err := conn.QueryRow("select 1::int4, pg_sleep(10)::varchar").Scan(&n, &s)
		if err == pgx.ErrDeadConn {
		} else if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Severity == "FATAL" {
		} else {
			t.Fatalf("Expected QueryRow Scan to return fatal PgError or ErrDeadConn, but instead received %v", err)
		}
	}()

	otherConn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer otherConn.Close()

	if _, err := otherConn.Exec(context.Background(), "select pg_terminate_backend($1)", conn.PID()); err != nil {
		t.Fatalf("Unable to kill backend PostgreSQL process: %v", err)
	}

	wg.Wait()

	if conn.IsAlive() {
		t.Fatal("Connection should not be live but was")
	}
}

func TestFatalTxError(t *testing.T) {
	t.Parallel()

	// Run timing sensitive test many times
	for i := 0; i < 50; i++ {
		func() {
			conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
			defer closeConn(t, conn)

			otherConn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
			defer otherConn.Close()

			_, err := otherConn.Exec(context.Background(), "select pg_terminate_backend($1)", conn.PID())
			if err != nil {
				t.Fatalf("Unable to kill backend PostgreSQL process: %v", err)
			}

			err = conn.QueryRow("select 1").Scan(nil)
			if err == nil {
				t.Fatal("Expected error but none occurred")
			}

			if conn.IsAlive() {
				t.Fatalf("Connection should not be live but was. Previous Query err: %v", err)
			}
		}()
	}
}

func TestInsertBoolArray(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	if results := mustExec(t, conn, "create temporary table foo(spice bool[]);"); string(results) != "CREATE TABLE" {
		t.Error("Unexpected results from Exec")
	}

	// Accept parameters
	if results := mustExec(t, conn, "insert into foo(spice) values($1)", []bool{true, false, true}); string(results) != "INSERT 0 1" {
		t.Errorf("Unexpected results from Exec: %v", results)
	}
}

func TestInsertTimestampArray(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	if results := mustExec(t, conn, "create temporary table foo(spice timestamp[]);"); string(results) != "CREATE TABLE" {
		t.Error("Unexpected results from Exec")
	}

	// Accept parameters
	if results := mustExec(t, conn, "insert into foo(spice) values($1)", []time.Time{time.Unix(1419143667, 0), time.Unix(1419143672, 0)}); string(results) != "INSERT 0 1" {
		t.Errorf("Unexpected results from Exec: %v", results)
	}
}

func TestCatchSimultaneousConnectionQueries(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	rows1, err := conn.Query("select generate_series(1,$1)", 10)
	if err != nil {
		t.Fatalf("conn.Query failed: %v", err)
	}
	defer rows1.Close()

	_, err = conn.Query("select generate_series(1,$1)", 10)
	if err != pgx.ErrConnBusy {
		t.Fatalf("conn.Query should have failed with pgx.ErrConnBusy, but it was %v", err)
	}
}

func TestCatchSimultaneousConnectionQueryAndExec(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	rows, err := conn.Query("select generate_series(1,$1)", 10)
	if err != nil {
		t.Fatalf("conn.Query failed: %v", err)
	}
	defer rows.Close()

	_, err = conn.Exec(context.Background(), "create temporary table foo(spice timestamp[])")
	if err != pgx.ErrConnBusy {
		t.Fatalf("conn.Exec should have failed with pgx.ErrConnBusy, but it was %v", err)
	}
}

type testLog struct {
	lvl  pgx.LogLevel
	msg  string
	data map[string]interface{}
}

type testLogger struct {
	logs []testLog
}

func (l *testLogger) Log(level pgx.LogLevel, msg string, data map[string]interface{}) {
	l.logs = append(l.logs, testLog{lvl: level, msg: msg, data: data})
}

func TestSetLogger(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	l1 := &testLogger{}
	oldLogger := conn.SetLogger(l1)
	if oldLogger != nil {
		t.Fatalf("Expected conn.SetLogger to return %v, but it was %v", nil, oldLogger)
	}

	if err := conn.Listen("foo"); err != nil {
		t.Fatal(err)
	}

	if len(l1.logs) == 0 {
		t.Fatal("Expected new logger l1 to be called, but it wasn't")
	}

	l2 := &testLogger{}
	oldLogger = conn.SetLogger(l2)
	if oldLogger != l1 {
		t.Fatalf("Expected conn.SetLogger to return %v, but it was %v", l1, oldLogger)
	}

	if err := conn.Listen("bar"); err != nil {
		t.Fatal(err)
	}

	if len(l2.logs) == 0 {
		t.Fatal("Expected new logger l2 to be called, but it wasn't")
	}
}

func TestSetLogLevel(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	logger := &testLogger{}
	conn.SetLogger(logger)

	if _, err := conn.SetLogLevel(0); err != pgx.ErrInvalidLogLevel {
		t.Fatal("SetLogLevel with invalid level did not return error")
	}

	if _, err := conn.SetLogLevel(pgx.LogLevelNone); err != nil {
		t.Fatal(err)
	}

	if err := conn.Listen("foo"); err != nil {
		t.Fatal(err)
	}

	if len(logger.logs) != 0 {
		t.Fatalf("Expected logger not to be called, but it was: %v", logger.logs)
	}

	if _, err := conn.SetLogLevel(pgx.LogLevelTrace); err != nil {
		t.Fatal(err)
	}

	if err := conn.Listen("bar"); err != nil {
		t.Fatal(err)
	}

	if len(logger.logs) == 0 {
		t.Fatal("Expected logger to be called, but it wasn't")
	}
}

func TestIdentifierSanitize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ident    pgx.Identifier
		expected string
	}{
		{
			ident:    pgx.Identifier{`foo`},
			expected: `"foo"`,
		},
		{
			ident:    pgx.Identifier{`select`},
			expected: `"select"`,
		},
		{
			ident:    pgx.Identifier{`foo`, `bar`},
			expected: `"foo"."bar"`,
		},
		{
			ident:    pgx.Identifier{`you should " not do this`},
			expected: `"you should "" not do this"`,
		},
		{
			ident:    pgx.Identifier{`you should " not do this`, `please don't`},
			expected: `"you should "" not do this"."please don't"`,
		},
	}

	for i, tt := range tests {
		qval := tt.ident.Sanitize()
		if qval != tt.expected {
			t.Errorf("%d. Expected Sanitize %v to return %v but it was %v", i, tt.ident, tt.expected, qval)
		}
	}
}

func TestConnInitConnInfo(t *testing.T) {
	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	// spot check that the standard postgres type names aren't qualified
	nameOIDs := map[string]pgtype.OID{
		"_int8": pgtype.Int8ArrayOID,
		"int8":  pgtype.Int8OID,
		"json":  pgtype.JSONOID,
		"text":  pgtype.TextOID,
	}
	for name, oid := range nameOIDs {
		dtByName, ok := conn.ConnInfo.DataTypeForName(name)
		if !ok {
			t.Fatalf("Expected type named %v to be present", name)
		}
		dtByOID, ok := conn.ConnInfo.DataTypeForOID(oid)
		if !ok {
			t.Fatalf("Expected type OID %v to be present", oid)
		}
		if dtByName != dtByOID {
			t.Fatalf("Expected type named %v to be the same as type OID %v", name, oid)
		}
	}

	ensureConnValid(t, conn)
}

func TestDomainType(t *testing.T) {
	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	dt, ok := conn.ConnInfo.DataTypeForName("uint64")
	if !ok {
		t.Fatal("Expected data type for domain uint64 to be present")
	}
	if dt, ok := dt.Value.(*pgtype.Numeric); !ok {
		t.Fatalf("Expected data type value for domain uint64 to be *pgtype.Numeric, but it was %T", dt)
	}

	var n uint64
	err := conn.QueryRow("select $1::uint64", uint64(42)).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}

	if n != 42 {
		t.Fatalf("Expected n to be 42, but was %v", n)
	}

	ensureConnValid(t, conn)
}
