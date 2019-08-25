package pgx_test

import (
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	errors "github.com/jackc/pgx/v4/errors"
	"github.com/stretchr/testify/assert"
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
	err = conn.QueryRow(context.Background(), "select 1 +1").Scan(&result)
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

	conn, err := pgx.ConnectConfig(context.Background(), config)
	if err != nil {
		t.Fatalf("Unable to establish connection: %v", err)
	}

	var currentDB string
	err = conn.QueryRow(context.Background(), "select current_database()").Scan(&currentDB)
	if err != nil {
		t.Fatalf("QueryRow Scan unexpectedly failed: %v", err)
	}
	if currentDB != config.Config.Database {
		t.Errorf("Did not connect to specified database (%v)", config.Config.Database)
	}

	var user string
	err = conn.QueryRow(context.Background(), "select current_user").Scan(&user)
	if err != nil {
		t.Fatalf("QueryRow Scan unexpectedly failed: %v", err)
	}
	if user != config.Config.User {
		t.Errorf("Did not connect as specified user (%v)", config.Config.User)
	}

	err = conn.Close(context.Background())
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
	err := conn.QueryRow(context.Background(), "select $1::int4", 42).Scan(&s)
	if err != nil {
		t.Fatal(err)
	}

	if s.Get() != "42" {
		t.Fatalf(`expected "42", got %v`, s)
	}

	ensureConnValid(t, conn)
}

func TestConnectConfigRequiresConnConfigFromParseConfig(t *testing.T) {
	t.Parallel()

	config := &pgx.ConnConfig{}

	require.PanicsWithValue(t, "config must be created by ParseConfig", func() { pgx.ConnectConfig(context.Background(), config) })
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

	rows, _ := conn.Query(context.Background(), "select 1")
	rows.Close()
	if rows.Err() != nil {
		t.Fatalf("Exec failure appears to have broken connection: %v", rows.Err())
	}
}

func TestExecFailureWithArguments(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	_, err := conn.Exec(context.Background(), "selct $1;", 1)
	if err == nil {
		t.Fatal("Expected SQL syntax error")
	}
	if errors.Is(err, pgconn.ErrNoBytesSent) {
		t.Error("Expected bytes to be sent to server")
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
		t.Fatalf("Unexpected results from Exec: %v", commandTag)
	}
	if errors.Is(err, pgconn.ErrNoBytesSent) {
		t.Error("Expected bytes to be sent to server")
	}
}

func TestExecContextFailureWithoutCancelation(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	_, err := conn.Exec(ctx, "selct;")
	if err == nil {
		t.Fatal("Expected SQL syntax error")
	}
	if errors.Is(err, pgconn.ErrNoBytesSent) {
		t.Error("Expected bytes to be sent to server")
	}

	rows, _ := conn.Query(context.Background(), "select 1")
	rows.Close()
	if rows.Err() != nil {
		t.Fatalf("ExecEx failure appears to have broken connection: %v", rows.Err())
	}
	if errors.Is(err, pgconn.ErrNoBytesSent) {
		t.Error("Expected bytes to be sent to server")
	}
}

func TestExecContextFailureWithoutCancelationWithArguments(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	_, err := conn.Exec(ctx, "selct $1;", 1)
	if err == nil {
		t.Fatal("Expected SQL syntax error")
	}
	if errors.Is(err, pgconn.ErrNoBytesSent) {
		t.Error("Expected bytes to be sent to server")
	}
}

func TestExecFailureCloseBefore(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	closeConn(t, conn)

	_, err := conn.Exec(context.Background(), "select 1")
	if err == nil {
		t.Fatal("Expected network error")
	}
	if !errors.Is(err, pgconn.ErrNoBytesSent) {
		t.Error("Expected no bytes to be sent to server")
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

	commandTag, err = conn.Exec(ctx,
		"insert into foo(name) values($1);",
		pgx.QuerySimpleProtocol(true),
		"bar'; drop table foo;--",
	)
	if err != nil {
		t.Fatal(err)
	}
	if string(commandTag) != "INSERT 0 1" {
		t.Fatalf("Unexpected results from Exec: %v", commandTag)
	}
}

func TestPrepare(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	_, err := conn.Prepare(context.Background(), "test", "select $1::varchar")
	if err != nil {
		t.Errorf("Unable to prepare statement: %v", err)
		return
	}

	var s string
	err = conn.QueryRow(context.Background(), "test", "hello").Scan(&s)
	if err != nil {
		t.Errorf("Executing prepared statement failed: %v", err)
	}

	if s != "hello" {
		t.Errorf("Prepared statement did not return expected value: %v", s)
	}

	err = conn.Deallocate(context.Background(), "test")
	if err != nil {
		t.Errorf("conn.Deallocate failed: %v", err)
	}

	// Create another prepared statement to ensure Deallocate left the connection
	// in a working state and that we can reuse the prepared statement name.

	_, err = conn.Prepare(context.Background(), "test", "select $1::integer")
	if err != nil {
		t.Errorf("Unable to prepare statement: %v", err)
		return
	}

	var n int32
	err = conn.QueryRow(context.Background(), "test", int32(1)).Scan(&n)
	if err != nil {
		t.Errorf("Executing prepared statement failed: %v", err)
	}

	if n != 1 {
		t.Errorf("Prepared statement did not return expected value: %v", s)
	}

	err = conn.Deallocate(context.Background(), "test")
	if err != nil {
		t.Errorf("conn.Deallocate failed: %v", err)
	}
}

func TestPrepareBadSQLFailure(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	if _, err := conn.Prepare(context.Background(), "badSQL", "select foo"); err == nil {
		t.Fatal("Prepare should have failed with syntax error")
	}

	ensureConnValid(t, conn)
}

func TestPrepareIdempotency(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	for i := 0; i < 2; i++ {
		_, err := conn.Prepare(context.Background(), "test", "select 42::integer")
		if err != nil {
			t.Fatalf("%d. Unable to prepare statement: %v", i, err)
		}

		var n int32
		err = conn.QueryRow(context.Background(), "test").Scan(&n)
		if err != nil {
			t.Errorf("%d. Executing prepared statement failed: %v", i, err)
		}

		if n != int32(42) {
			t.Errorf("%d. Prepared statement did not return expected value: %v", i, n)
		}
	}

	_, err := conn.Prepare(context.Background(), "test", "select 'fail'::varchar")
	if err == nil {
		t.Fatalf("Prepare statement with same name but different SQL should have failed but it didn't")
		return
	}
}

func TestListenNotify(t *testing.T) {
	t.Parallel()

	listener := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, listener)

	mustExec(t, listener, "listen chat")

	notifier := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, notifier)

	mustExec(t, notifier, "notify chat")

	// when notification is waiting on the socket to be read
	notification, err := listener.WaitForNotification(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "chat", notification.Channel)

	// when notification has already been read during previous query
	mustExec(t, notifier, "notify chat")
	rows, _ := listener.Query(context.Background(), "select 1")
	rows.Close()
	require.NoError(t, rows.Err())

	ctx, cancelFn := context.WithCancel(context.Background())
	cancelFn()
	notification, err = listener.WaitForNotification(ctx)
	require.NoError(t, err)
	assert.Equal(t, "chat", notification.Channel)

	// when timeout occurs
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	notification, err = listener.WaitForNotification(ctx)
	assert.True(t, errors.Is(err, context.DeadlineExceeded))

	// listener can listen again after a timeout
	mustExec(t, notifier, "notify chat")
	notification, err = listener.WaitForNotification(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "chat", notification.Channel)
}

func TestListenNotifyWhileBusyIsSafe(t *testing.T) {
	t.Parallel()

	listenerDone := make(chan bool)
	go func() {
		conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
		defer closeConn(t, conn)
		defer func() {
			listenerDone <- true
		}()

		mustExec(t, conn, "listen busysafe")

		for i := 0; i < 5000; i++ {
			var sum int32
			var rowCount int32

			rows, err := conn.Query(context.Background(), "select generate_series(1,$1)", 100)
			if err != nil {
				t.Fatalf("conn.Query failed: %v", err)
			}

			for rows.Next() {
				var n int32
				rows.Scan(&n)
				sum += n
				rowCount++
			}

			if rows.Err() != nil {
				t.Fatalf("conn.Query failed: %v", err)
			}

			if sum != 5050 {
				t.Fatalf("Wrong rows sum: %v", sum)
			}

			if rowCount != 100 {
				t.Fatalf("Wrong number of rows: %v", rowCount)
			}

			time.Sleep(1 * time.Microsecond)
		}
	}()

	go func() {
		conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
		defer closeConn(t, conn)

		for i := 0; i < 100000; i++ {
			mustExec(t, conn, "notify busysafe, 'hello'")
			time.Sleep(1 * time.Microsecond)
		}
	}()

	<-listenerDone
}

func TestListenNotifySelfNotification(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	mustExec(t, conn, "listen self")

	// Notify self and WaitForNotification immediately
	mustExec(t, conn, "notify self")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	notification, err := conn.WaitForNotification(ctx)
	require.NoError(t, err)
	assert.Equal(t, "self", notification.Channel)

	// Notify self and do something else before WaitForNotification
	mustExec(t, conn, "notify self")

	rows, _ := conn.Query(context.Background(), "select 1")
	rows.Close()
	if rows.Err() != nil {
		t.Fatalf("Unexpected error on Query: %v", rows.Err())
	}

	ctx, cncl := context.WithTimeout(context.Background(), time.Second)
	defer cncl()
	notification, err = conn.WaitForNotification(ctx)
	require.NoError(t, err)
	assert.Equal(t, "self", notification.Channel)
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
		err := conn.QueryRow(context.Background(), "select 1::int4, pg_sleep(10)::varchar").Scan(&n, &s)
		if err == pgx.ErrDeadConn {
		} else if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Severity == "FATAL" {
		} else {
			t.Fatalf("Expected QueryRow Scan to return fatal PgError or ErrDeadConn, but instead received %v", err)
		}
	}()

	otherConn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer otherConn.Close(context.Background())

	if _, err := otherConn.Exec(context.Background(), "select pg_terminate_backend($1)", conn.PgConn().PID()); err != nil {
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
			defer otherConn.Close(context.Background())

			_, err := otherConn.Exec(context.Background(), "select pg_terminate_backend($1)", conn.PgConn().PID())
			if err != nil {
				t.Fatalf("Unable to kill backend PostgreSQL process: %v", err)
			}

			err = conn.QueryRow(context.Background(), "select 1").Scan(nil)
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

	rows1, err := conn.Query(context.Background(), "select generate_series(1,$1)", 10)
	if err != nil {
		t.Fatalf("conn.Query failed: %v", err)
	}
	defer rows1.Close()

	_, err = conn.Query(context.Background(), "select generate_series(1,$1)", 10)
	if !errors.Is(err, pgconn.ErrConnBusy) {
		t.Fatalf("conn.Query should have failed with pgconn.ErrConnBusy, but it was %v", err)
	}
}

func TestCatchSimultaneousConnectionQueryAndExec(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	rows, err := conn.Query(context.Background(), "select generate_series(1,$1)", 10)
	if err != nil {
		t.Fatalf("conn.Query failed: %v", err)
	}
	defer rows.Close()

	_, err = conn.Exec(context.Background(), "create temporary table foo(spice timestamp[])")
	if !errors.Is(err, pgconn.ErrConnBusy) {
		t.Fatalf("conn.Exec should have failed with pgconn.ErrConnBusy, but it was %v", err)
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

func (l *testLogger) Log(ctx context.Context, level pgx.LogLevel, msg string, data map[string]interface{}) {
	data["ctxdata"] = ctx.Value("ctxdata")
	l.logs = append(l.logs, testLog{lvl: level, msg: msg, data: data})
}

func TestLogPassesContext(t *testing.T) {
	t.Parallel()

	l1 := &testLogger{}
	config := mustParseConfig(t, os.Getenv("PGX_TEST_DATABASE"))
	config.Logger = l1

	conn := mustConnect(t, config)
	defer closeConn(t, conn)

	l1.logs = l1.logs[0:0] // Clear logs written when establishing connection

	ctx := context.WithValue(context.Background(), "ctxdata", "foo")

	if _, err := conn.Exec(ctx, ";"); err != nil {
		t.Fatal(err)
	}

	if len(l1.logs) != 1 {
		t.Fatal("Expected logger to be called once, but it wasn't")
	}

	if l1.logs[0].data["ctxdata"] != "foo" {
		t.Fatal("Expected context data to be passed to logger, but it wasn't")
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
		{
			ident:    pgx.Identifier{`you should ` + string([]byte{0}) + `not do this`},
			expected: `"you should not do this"`,
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
	nameOIDs := map[string]uint32{
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

func TestUnregisteredTypeUsableAsStringArgumentAndBaseResult(t *testing.T) {
	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	var n uint64
	err := conn.QueryRow(context.Background(), "select $1::uint64", "42").Scan(&n)
	if err != nil {
		t.Fatal(err)
	}

	if n != 42 {
		t.Fatalf("Expected n to be 42, but was %v", n)
	}

	ensureConnValid(t, conn)
}

func TestDomainType(t *testing.T) {
	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	var err error
	var n uint64

	// Domain type uint64 is a PostgreSQL domain of underlying type numeric.

	// Since it is not registered, pgx does not know how to encode Go uint64 argument.
	err = conn.QueryRow(context.Background(), "select $1::uint64", uint64(24)).Scan(&n)
	if err == nil {
		t.Fatal("expected error encoding uint64 into unregistered domain")
	}

	// A string can be used. But a string cannot be the result because the describe result from the PostgreSQL server gives
	// the underlying type of numeric.
	err = conn.QueryRow(context.Background(), "select $1::uint64", "42").Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	if n != 42 {
		t.Fatalf("Expected n to be 42, but was %v", n)
	}

	var uint64OID uint32
	err = conn.QueryRow(context.Background(), "select t.oid from pg_type t where t.typname='uint64';").Scan(&uint64OID)
	if err != nil {
		t.Fatalf("did not find uint64 OID, %v", err)
	}
	conn.ConnInfo.RegisterDataType(pgtype.DataType{Value: &pgtype.Numeric{}, Name: "uint64", OID: uint64OID})

	// String is still an acceptable argument after registration
	err = conn.QueryRow(context.Background(), "select $1::uint64", "7").Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	if n != 7 {
		t.Fatalf("Expected n to be 7, but was %v", n)
	}

	// But a uint64 is acceptable
	err = conn.QueryRow(context.Background(), "select $1::uint64", uint64(24)).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	if n != 24 {
		t.Fatalf("Expected n to be 24, but was %v", n)
	}

	ensureConnValid(t, conn)
}
