package pgx_test

import (
	"bytes"
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxtest"
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

	assert.Equal(t, connString, conn.Config().ConnString())

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

	connString := os.Getenv("PGX_TEST_DATABASE")
	config := mustParseConfig(t, connString)

	conn, err := pgx.ConnectConfig(context.Background(), config)
	if err != nil {
		t.Fatalf("Unable to establish connection: %v", err)
	}

	assertConfigsEqual(t, config, conn.Config(), "Conn.Config() returns original config")

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
	connConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	conn := mustConnect(t, connConfig)
	defer closeConn(t, conn)

	// If simple protocol is used we should be able to correctly scan the result
	// into a pgtype.Text as the integer will have been encoded in text.

	var s pgtype.Text
	err := conn.QueryRow(context.Background(), "select $1::int4", 42).Scan(&s)
	require.NoError(t, err)
	require.Equal(t, pgtype.Text{String: "42", Valid: true}, s)

	ensureConnValid(t, conn)
}

func TestConnectConfigRequiresConnConfigFromParseConfig(t *testing.T) {
	config := &pgx.ConnConfig{}
	require.PanicsWithValue(t, "config must be created by ParseConfig", func() {
		pgx.ConnectConfig(context.Background(), config)
	})
}

func TestConfigContainsConnStr(t *testing.T) {
	connStr := os.Getenv("PGX_TEST_DATABASE")
	config, err := pgx.ParseConfig(connStr)
	require.NoError(t, err)
	assert.Equal(t, connStr, config.ConnString())
}

func TestConfigCopyReturnsEqualConfig(t *testing.T) {
	connString := "postgres://jack:secret@localhost:5432/mydb?application_name=pgxtest&search_path=myschema&connect_timeout=5"
	original, err := pgx.ParseConfig(connString)
	require.NoError(t, err)

	copied := original.Copy()
	assertConfigsEqual(t, original, copied, t.Name())
}

func TestConfigCopyCanBeUsedToConnect(t *testing.T) {
	connString := os.Getenv("PGX_TEST_DATABASE")
	original, err := pgx.ParseConfig(connString)
	require.NoError(t, err)

	copied := original.Copy()
	assert.NotPanics(t, func() {
		_, err = pgx.ConnectConfig(context.Background(), copied)
	})
	assert.NoError(t, err)
}

func TestParseConfigExtractsStatementCacheOptions(t *testing.T) {
	t.Parallel()

	config, err := pgx.ParseConfig("statement_cache_capacity=0")
	require.NoError(t, err)
	require.EqualValues(t, 0, config.StatementCacheCapacity)

	config, err = pgx.ParseConfig("statement_cache_capacity=42")
	require.NoError(t, err)
	require.EqualValues(t, 42, config.StatementCacheCapacity)

	config, err = pgx.ParseConfig("description_cache_capacity=0")
	require.NoError(t, err)
	require.EqualValues(t, 0, config.DescriptionCacheCapacity)

	config, err = pgx.ParseConfig("description_cache_capacity=42")
	require.NoError(t, err)
	require.EqualValues(t, 42, config.DescriptionCacheCapacity)

	//	default_query_exec_mode
	//		Possible values: "cache_statement", "cache_describe", "describe_exec", "exec", and "simple_protocol". See

	config, err = pgx.ParseConfig("default_query_exec_mode=cache_statement")
	require.NoError(t, err)
	require.Equal(t, pgx.QueryExecModeCacheStatement, config.DefaultQueryExecMode)

	config, err = pgx.ParseConfig("default_query_exec_mode=cache_describe")
	require.NoError(t, err)
	require.Equal(t, pgx.QueryExecModeCacheDescribe, config.DefaultQueryExecMode)

	config, err = pgx.ParseConfig("default_query_exec_mode=describe_exec")
	require.NoError(t, err)
	require.Equal(t, pgx.QueryExecModeDescribeExec, config.DefaultQueryExecMode)

	config, err = pgx.ParseConfig("default_query_exec_mode=exec")
	require.NoError(t, err)
	require.Equal(t, pgx.QueryExecModeExec, config.DefaultQueryExecMode)

	config, err = pgx.ParseConfig("default_query_exec_mode=simple_protocol")
	require.NoError(t, err)
	require.Equal(t, pgx.QueryExecModeSimpleProtocol, config.DefaultQueryExecMode)
}

func TestParseConfigExtractsDefaultQueryExecMode(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		connString           string
		defaultQueryExecMode pgx.QueryExecMode
	}{
		{"", pgx.QueryExecModeCacheStatement},
		{"default_query_exec_mode=cache_statement", pgx.QueryExecModeCacheStatement},
		{"default_query_exec_mode=cache_describe", pgx.QueryExecModeCacheDescribe},
		{"default_query_exec_mode=describe_exec", pgx.QueryExecModeDescribeExec},
		{"default_query_exec_mode=exec", pgx.QueryExecModeExec},
		{"default_query_exec_mode=simple_protocol", pgx.QueryExecModeSimpleProtocol},
	} {
		config, err := pgx.ParseConfig(tt.connString)
		require.NoError(t, err)
		require.Equalf(t, tt.defaultQueryExecMode, config.DefaultQueryExecMode, "connString: `%s`", tt.connString)
		require.Empty(t, config.RuntimeParams["default_query_exec_mode"])
	}
}

func TestExec(t *testing.T) {
	t.Parallel()

	pgxtest.RunWithQueryExecModes(context.Background(), t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		if results := mustExec(t, conn, "create temporary table foo(id integer primary key);"); results.String() != "CREATE TABLE" {
			t.Error("Unexpected results from Exec")
		}

		// Accept parameters
		if results := mustExec(t, conn, "insert into foo(id) values($1)", 1); results.String() != "INSERT 0 1" {
			t.Errorf("Unexpected results from Exec: %v", results)
		}

		if results := mustExec(t, conn, "drop table foo;"); results.String() != "DROP TABLE" {
			t.Error("Unexpected results from Exec")
		}

		// Multiple statements can be executed -- last command tag is returned
		if results := mustExec(t, conn, "create temporary table foo(id serial primary key); drop table foo;"); results.String() != "DROP TABLE" {
			t.Error("Unexpected results from Exec")
		}

		// Can execute longer SQL strings than sharedBufferSize
		if results := mustExec(t, conn, strings.Repeat("select 42; ", 1000)); results.String() != "SELECT 1" {
			t.Errorf("Unexpected results from Exec: %v", results)
		}

		// Exec no-op which does not return a command tag
		if results := mustExec(t, conn, "--;"); results.String() != "" {
			t.Errorf("Unexpected results from Exec: %v", results)
		}
	})
}

type testQueryRewriter struct {
	sql  string
	args []any
}

func (qr *testQueryRewriter) RewriteQuery(ctx context.Context, conn *pgx.Conn, sql string, args []any) (newSQL string, newArgs []any, err error) {
	return qr.sql, qr.args, nil
}

func TestExecWithQueryRewriter(t *testing.T) {
	t.Parallel()

	pgxtest.RunWithQueryExecModes(context.Background(), t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		qr := testQueryRewriter{sql: "select $1::int", args: []any{42}}
		_, err := conn.Exec(ctx, "should be replaced", &qr)
		require.NoError(t, err)
	})
}

func TestExecFailure(t *testing.T) {
	t.Parallel()

	pgxtest.RunWithQueryExecModes(context.Background(), t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		if _, err := conn.Exec(context.Background(), "selct;"); err == nil {
			t.Fatal("Expected SQL syntax error")
		}

		rows, _ := conn.Query(context.Background(), "select 1")
		rows.Close()
		if rows.Err() != nil {
			t.Fatalf("Exec failure appears to have broken connection: %v", rows.Err())
		}
	})
}

func TestExecFailureWithArguments(t *testing.T) {
	t.Parallel()

	pgxtest.RunWithQueryExecModes(context.Background(), t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		_, err := conn.Exec(context.Background(), "selct $1;", 1)
		if err == nil {
			t.Fatal("Expected SQL syntax error")
		}
		assert.False(t, pgconn.SafeToRetry(err))

		_, err = conn.Exec(context.Background(), "select $1::varchar(1);", "1", "2")
		require.Error(t, err)
	})
}

func TestExecContextWithoutCancelation(t *testing.T) {
	t.Parallel()

	pgxtest.RunWithQueryExecModes(context.Background(), t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		ctx, cancelFunc := context.WithCancel(context.Background())
		defer cancelFunc()

		commandTag, err := conn.Exec(ctx, "create temporary table foo(id integer primary key);")
		if err != nil {
			t.Fatal(err)
		}
		if commandTag.String() != "CREATE TABLE" {
			t.Fatalf("Unexpected results from Exec: %v", commandTag)
		}
		assert.False(t, pgconn.SafeToRetry(err))
	})
}

func TestExecContextFailureWithoutCancelation(t *testing.T) {
	t.Parallel()

	pgxtest.RunWithQueryExecModes(context.Background(), t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		ctx, cancelFunc := context.WithCancel(context.Background())
		defer cancelFunc()

		_, err := conn.Exec(ctx, "selct;")
		if err == nil {
			t.Fatal("Expected SQL syntax error")
		}
		assert.False(t, pgconn.SafeToRetry(err))

		rows, _ := conn.Query(context.Background(), "select 1")
		rows.Close()
		if rows.Err() != nil {
			t.Fatalf("ExecEx failure appears to have broken connection: %v", rows.Err())
		}
		assert.False(t, pgconn.SafeToRetry(err))
	})
}

func TestExecContextFailureWithoutCancelationWithArguments(t *testing.T) {
	t.Parallel()

	pgxtest.RunWithQueryExecModes(context.Background(), t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		ctx, cancelFunc := context.WithCancel(context.Background())
		defer cancelFunc()

		_, err := conn.Exec(ctx, "selct $1;", 1)
		if err == nil {
			t.Fatal("Expected SQL syntax error")
		}
		assert.False(t, pgconn.SafeToRetry(err))
	})
}

func TestExecFailureCloseBefore(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	closeConn(t, conn)

	_, err := conn.Exec(context.Background(), "select 1")
	require.Error(t, err)
	assert.True(t, pgconn.SafeToRetry(err))
}

func TestExecPerQuerySimpleProtocol(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	commandTag, err := conn.Exec(ctx, "create temporary table foo(name varchar primary key);")
	if err != nil {
		t.Fatal(err)
	}
	if commandTag.String() != "CREATE TABLE" {
		t.Fatalf("Unexpected results from Exec: %v", commandTag)
	}

	commandTag, err = conn.Exec(ctx,
		"insert into foo(name) values($1);",
		pgx.QueryExecModeSimpleProtocol,
		"bar'; drop table foo;--",
	)
	if err != nil {
		t.Fatal(err)
	}
	if commandTag.String() != "INSERT 0 1" {
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

	err = conn.DeallocateAll(context.Background())
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

func TestPrepareStatementCacheModes(t *testing.T) {
	t.Parallel()

	pgxtest.RunWithQueryExecModes(context.Background(), t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		_, err := conn.Prepare(context.Background(), "test", "select $1::text")
		require.NoError(t, err)

		var s string
		err = conn.QueryRow(context.Background(), "test", "hello").Scan(&s)
		require.NoError(t, err)
		require.Equal(t, "hello", s)
	})
}

func TestListenNotify(t *testing.T) {
	t.Parallel()

	listener := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, listener)

	if listener.PgConn().ParameterStatus("crdb_version") != "" {
		t.Skip("Server does not support LISTEN / NOTIFY (https://github.com/cockroachdb/cockroach/issues/41522)")
	}

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
	assert.True(t, pgconn.Timeout(err))

	// listener can listen again after a timeout
	mustExec(t, notifier, "notify chat")
	notification, err = listener.WaitForNotification(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "chat", notification.Channel)
}

func TestListenNotifyWhileBusyIsSafe(t *testing.T) {
	t.Parallel()

	func() {
		conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
		defer closeConn(t, conn)
		pgxtest.SkipCockroachDB(t, conn, "Server does not support LISTEN / NOTIFY (https://github.com/cockroachdb/cockroach/issues/41522)")
	}()

	listenerDone := make(chan bool)
	notifierDone := make(chan bool)
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
				t.Errorf("conn.Query failed: %v", err)
				return
			}

			for rows.Next() {
				var n int32
				if err := rows.Scan(&n); err != nil {
					t.Errorf("Row scan failed: %v", err)
					return
				}
				sum += n
				rowCount++
			}

			if rows.Err() != nil {
				t.Errorf("conn.Query failed: %v", rows.Err())
				return
			}

			if sum != 5050 {
				t.Errorf("Wrong rows sum: %v", sum)
				return
			}

			if rowCount != 100 {
				t.Errorf("Wrong number of rows: %v", rowCount)
				return
			}

			time.Sleep(1 * time.Microsecond)
		}
	}()

	go func() {
		conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
		defer closeConn(t, conn)
		defer func() {
			notifierDone <- true
		}()

		for i := 0; i < 100000; i++ {
			mustExec(t, conn, "notify busysafe, 'hello'")
			time.Sleep(1 * time.Microsecond)
		}
	}()

	<-listenerDone
	<-notifierDone
}

func TestListenNotifySelfNotification(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	pgxtest.SkipCockroachDB(t, conn, "Server does not support LISTEN / NOTIFY (https://github.com/cockroachdb/cockroach/issues/41522)")

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

	pgxtest.SkipCockroachDB(t, conn, "Server does not support pg_terminate_backend() (https://github.com/cockroachdb/cockroach/issues/35897)")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		var n int32
		var s string
		err := conn.QueryRow(context.Background(), "select 1::int4, pg_sleep(10)::varchar").Scan(&n, &s)
		if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Severity == "FATAL" {
		} else {
			t.Errorf("Expected QueryRow Scan to return fatal PgError, but instead received %v", err)
			return
		}
	}()

	otherConn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer otherConn.Close(context.Background())

	if _, err := otherConn.Exec(context.Background(), "select pg_terminate_backend($1)", conn.PgConn().PID()); err != nil {
		t.Fatalf("Unable to kill backend PostgreSQL process: %v", err)
	}

	wg.Wait()

	if !conn.IsClosed() {
		t.Fatal("Connection should be closed")
	}
}

func TestFatalTxError(t *testing.T) {
	t.Parallel()

	// Run timing sensitive test many times
	for i := 0; i < 50; i++ {
		func() {
			conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
			defer closeConn(t, conn)

			pgxtest.SkipCockroachDB(t, conn, "Server does not support pg_terminate_backend() (https://github.com/cockroachdb/cockroach/issues/35897)")

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

			if !conn.IsClosed() {
				t.Fatalf("Connection should be closed but isn't. Previous Query err: %v", err)
			}
		}()
	}
}

func TestInsertBoolArray(t *testing.T) {
	t.Parallel()

	pgxtest.RunWithQueryExecModes(context.Background(), t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		if results := mustExec(t, conn, "create temporary table foo(spice bool[]);"); results.String() != "CREATE TABLE" {
			t.Error("Unexpected results from Exec")
		}

		// Accept parameters
		if results := mustExec(t, conn, "insert into foo(spice) values($1)", []bool{true, false, true}); results.String() != "INSERT 0 1" {
			t.Errorf("Unexpected results from Exec: %v", results)
		}
	})
}

func TestInsertTimestampArray(t *testing.T) {
	t.Parallel()

	pgxtest.RunWithQueryExecModes(context.Background(), t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		if results := mustExec(t, conn, "create temporary table foo(spice timestamp[]);"); results.String() != "CREATE TABLE" {
			t.Error("Unexpected results from Exec")
		}

		// Accept parameters
		if results := mustExec(t, conn, "insert into foo(spice) values($1)", []time.Time{time.Unix(1419143667, 0), time.Unix(1419143672, 0)}); results.String() != "INSERT 0 1" {
			t.Errorf("Unexpected results from Exec: %v", results)
		}
	})
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

func TestConnInitTypeMap(t *testing.T) {
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
		dtByName, ok := conn.TypeMap().TypeForName(name)
		if !ok {
			t.Fatalf("Expected type named %v to be present", name)
		}
		dtByOID, ok := conn.TypeMap().TypeForOID(oid)
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
	pgxtest.RunWithQueryExecModes(context.Background(), t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		pgxtest.SkipCockroachDB(t, conn, "Server does support domain types (https://github.com/cockroachdb/cockroach/issues/27796)")

		var n uint64
		err := conn.QueryRow(context.Background(), "select $1::uint64", "42").Scan(&n)
		if err != nil {
			t.Fatal(err)
		}

		if n != 42 {
			t.Fatalf("Expected n to be 42, but was %v", n)
		}
	})
}

func TestDomainType(t *testing.T) {
	pgxtest.RunWithQueryExecModes(context.Background(), t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		pgxtest.SkipCockroachDB(t, conn, "Server does support domain types (https://github.com/cockroachdb/cockroach/issues/27796)")

		// Domain type uint64 is a PostgreSQL domain of underlying type numeric.

		// In the extended protocol preparing "select $1::uint64" appears to create a statement that expects a param OID of
		// uint64 but a result OID of the underlying numeric.

		var s string
		err := conn.QueryRow(ctx, "select $1::uint64", "24").Scan(&s)
		require.NoError(t, err)
		require.Equal(t, "24", s)

		// Register type
		uint64Type, err := conn.LoadType(ctx, "uint64")
		require.NoError(t, err)
		conn.TypeMap().RegisterType(uint64Type)

		var n uint64
		err = conn.QueryRow(ctx, "select $1::uint64", uint64(24)).Scan(&n)
		require.NoError(t, err)

		// String is still an acceptable argument after registration
		err = conn.QueryRow(ctx, "select $1::uint64", "7").Scan(&n)
		if err != nil {
			t.Fatal(err)
		}
		if n != 7 {
			t.Fatalf("Expected n to be 7, but was %v", n)
		}
	})
}

func TestLoadTypeSameNameInDifferentSchemas(t *testing.T) {
	pgxtest.RunWithQueryExecModes(context.Background(), t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		pgxtest.SkipCockroachDB(t, conn, "Server does support composite types (https://github.com/cockroachdb/cockroach/issues/27792)")

		tx, err := conn.Begin(ctx)
		require.NoError(t, err)
		defer tx.Rollback(ctx)

		_, err = tx.Exec(ctx, `create schema pgx_a;
create type pgx_a.point as (a text, b text);
create schema pgx_b;
create type pgx_b.point as (c text);
`)
		require.NoError(t, err)

		// Register types
		for _, typename := range []string{"pgx_a.point", "pgx_b.point"} {
			// Obviously using conn while a tx is in use and registering a type after the connection has been established are
			// really bad practices, but for the sake of convenience we do it in the test here.
			dt, err := conn.LoadType(ctx, typename)
			require.NoError(t, err)
			conn.TypeMap().RegisterType(dt)
		}

		type aPoint struct {
			A string
			B string
		}

		type bPoint struct {
			C string
		}

		var a aPoint
		var b bPoint
		err = tx.QueryRow(ctx, `select '(foo,bar)'::pgx_a.point, '(baz)'::pgx_b.point`).Scan(&a, &b)
		require.NoError(t, err)
		require.Equal(t, aPoint{"foo", "bar"}, a)
		require.Equal(t, bPoint{"baz"}, b)
	})
}

func TestLoadRangeType(t *testing.T) {
	pgxtest.RunWithQueryExecModes(context.Background(), t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		pgxtest.SkipCockroachDB(t, conn, "Server does support range types")

		tx, err := conn.Begin(ctx)
		require.NoError(t, err)
		defer tx.Rollback(ctx)

		_, err = tx.Exec(ctx, "create type examplefloatrange as range (subtype=float8, subtype_diff=float8mi)")
		require.NoError(t, err)

		// Register types
		newRangeType, err := conn.LoadType(ctx, "examplefloatrange")
		require.NoError(t, err)
		conn.TypeMap().RegisterType(newRangeType)
		conn.TypeMap().RegisterDefaultPgType(pgtype.Range[float64]{}, "examplefloatrange")

		var inputRangeType = pgtype.Range[float64]{
			Lower:     1.0,
			Upper:     2.0,
			LowerType: pgtype.Inclusive,
			UpperType: pgtype.Inclusive,
			Valid:     true,
		}
		var outputRangeType pgtype.Range[float64]
		err = tx.QueryRow(ctx, "SELECT $1::examplefloatrange", inputRangeType).Scan(&outputRangeType)
		require.NoError(t, err)
		require.Equal(t, inputRangeType, outputRangeType)
	})
}

func TestLoadMultiRangeType(t *testing.T) {
	pgxtest.RunWithQueryExecModes(context.Background(), t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		pgxtest.SkipCockroachDB(t, conn, "Server does support range types")
		pgxtest.SkipPostgreSQLVersionLessThan(t, conn, 14) // multirange data type was added in 14 postgresql

		tx, err := conn.Begin(ctx)
		require.NoError(t, err)
		defer tx.Rollback(ctx)

		_, err = tx.Exec(ctx, "create type examplefloatrange as range (subtype=float8, subtype_diff=float8mi, multirange_type_name=examplefloatmultirange)")
		require.NoError(t, err)

		// Register types
		newRangeType, err := conn.LoadType(ctx, "examplefloatrange")
		require.NoError(t, err)
		conn.TypeMap().RegisterType(newRangeType)
		conn.TypeMap().RegisterDefaultPgType(pgtype.Range[float64]{}, "examplefloatrange")

		newMultiRangeType, err := conn.LoadType(ctx, "examplefloatmultirange")
		require.NoError(t, err)
		conn.TypeMap().RegisterType(newMultiRangeType)
		conn.TypeMap().RegisterDefaultPgType(pgtype.Multirange[pgtype.Range[float64]]{}, "examplefloatmultirange")

		var inputMultiRangeType = pgtype.Multirange[pgtype.Range[float64]]{
			{
				Lower:     1.0,
				Upper:     2.0,
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Inclusive,
				Valid:     true,
			},
			{
				Lower:     3.0,
				Upper:     4.0,
				LowerType: pgtype.Exclusive,
				UpperType: pgtype.Exclusive,
				Valid:     true,
			},
		}
		var outputMultiRangeType pgtype.Multirange[pgtype.Range[float64]]
		err = tx.QueryRow(ctx, "SELECT $1::examplefloatmultirange", inputMultiRangeType).Scan(&outputMultiRangeType)
		require.NoError(t, err)
		require.Equal(t, inputMultiRangeType, outputMultiRangeType)
	})
}

func TestStmtCacheInvalidationConn(t *testing.T) {
	ctx := context.Background()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	// create a table and fill it with some data
	_, err := conn.Exec(ctx, `
        DROP TABLE IF EXISTS drop_cols;
        CREATE TABLE drop_cols (
            id SERIAL PRIMARY KEY NOT NULL,
            f1 int NOT NULL,
            f2 int NOT NULL
        );
    `)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, "INSERT INTO drop_cols (f1, f2) VALUES (1, 2)")
	require.NoError(t, err)

	getSQL := "SELECT * FROM drop_cols WHERE id = $1"

	// This query will populate the statement cache. We don't care about the result.
	rows, err := conn.Query(ctx, getSQL, 1)
	require.NoError(t, err)
	rows.Close()
	require.NoError(t, rows.Err())

	// Now, change the schema of the table out from under the statement, making it invalid.
	_, err = conn.Exec(ctx, "ALTER TABLE drop_cols DROP COLUMN f1")
	require.NoError(t, err)

	// We must get an error the first time we try to re-execute a bad statement.
	// It is up to the application to determine if it wants to try again. We punt to
	// the application because there is no clear recovery path in the case of failed transactions
	// or batch operations and because automatic retry is tricky and we don't want to get
	// it wrong at such an importaint layer of the stack.
	rows, err = conn.Query(ctx, getSQL, 1)
	require.NoError(t, err)
	rows.Next()
	nextErr := rows.Err()
	rows.Close()
	for _, err := range []error{nextErr, rows.Err()} {
		if err == nil {
			t.Fatal(`expected "cached plan must not change result type": no error`)
		}
		if !strings.Contains(err.Error(), "cached plan must not change result type") {
			t.Fatalf(`expected "cached plan must not change result type", got: "%s"`, err.Error())
		}
	}

	// On retry, the statement should have been flushed from the cache.
	rows, err = conn.Query(ctx, getSQL, 1)
	require.NoError(t, err)
	rows.Next()
	err = rows.Err()
	require.NoError(t, err)
	rows.Close()
	require.NoError(t, rows.Err())

	ensureConnValid(t, conn)
}

func TestStmtCacheInvalidationTx(t *testing.T) {
	ctx := context.Background()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	if conn.PgConn().ParameterStatus("crdb_version") != "" {
		t.Skip("Server has non-standard prepare in errored transaction behavior (https://github.com/cockroachdb/cockroach/issues/84140)")
	}

	// create a table and fill it with some data
	_, err := conn.Exec(ctx, `
        DROP TABLE IF EXISTS drop_cols;
        CREATE TABLE drop_cols (
            id SERIAL PRIMARY KEY NOT NULL,
            f1 int NOT NULL,
            f2 int NOT NULL
        );
    `)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, "INSERT INTO drop_cols (f1, f2) VALUES (1, 2)")
	require.NoError(t, err)

	tx, err := conn.Begin(ctx)
	require.NoError(t, err)

	getSQL := "SELECT * FROM drop_cols WHERE id = $1"

	// This query will populate the statement cache. We don't care about the result.
	rows, err := tx.Query(ctx, getSQL, 1)
	require.NoError(t, err)
	rows.Close()
	require.NoError(t, rows.Err())

	// Now, change the schema of the table out from under the statement, making it invalid.
	_, err = tx.Exec(ctx, "ALTER TABLE drop_cols DROP COLUMN f1")
	require.NoError(t, err)

	// We must get an error the first time we try to re-execute a bad statement.
	// It is up to the application to determine if it wants to try again. We punt to
	// the application because there is no clear recovery path in the case of failed transactions
	// or batch operations and because automatic retry is tricky and we don't want to get
	// it wrong at such an importaint layer of the stack.
	rows, err = tx.Query(ctx, getSQL, 1)
	require.NoError(t, err)
	rows.Next()
	nextErr := rows.Err()
	rows.Close()
	for _, err := range []error{nextErr, rows.Err()} {
		if err == nil {
			t.Fatal(`expected "cached plan must not change result type": no error`)
		}
		if !strings.Contains(err.Error(), "cached plan must not change result type") {
			t.Fatalf(`expected "cached plan must not change result type", got: "%s"`, err.Error())
		}
	}

	rows, _ = tx.Query(ctx, getSQL, 1)
	rows.Close()
	err = rows.Err()
	// Retries within the same transaction are errors (really anything except a rollback
	// will be an error in this transaction).
	require.Error(t, err)
	rows.Close()

	err = tx.Rollback(ctx)
	require.NoError(t, err)

	// once we've rolled back, retries will work
	rows, err = conn.Query(ctx, getSQL, 1)
	require.NoError(t, err)
	rows.Next()
	err = rows.Err()
	require.NoError(t, err)
	rows.Close()

	ensureConnValid(t, conn)
}

func TestInsertDurationInterval(t *testing.T) {
	pgxtest.RunWithQueryExecModes(context.Background(), t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		_, err := conn.Exec(context.Background(), "create temporary table t(duration INTERVAL(0) NOT NULL)")
		require.NoError(t, err)

		result, err := conn.Exec(context.Background(), "insert into t(duration) values($1)", time.Minute)
		require.NoError(t, err)

		n := result.RowsAffected()
		require.EqualValues(t, 1, n)
	})
}

func TestRawValuesUnderlyingMemoryReused(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		var buf []byte

		rows, err := conn.Query(ctx, `select 1::int`)
		require.NoError(t, err)

		for rows.Next() {
			buf = rows.RawValues()[0]
		}

		require.NoError(t, rows.Err())

		original := make([]byte, len(buf))
		copy(original, buf)

		for i := 0; i < 1_000_000; i++ {
			rows, err := conn.Query(ctx, `select $1::int`, i)
			require.NoError(t, err)
			rows.Close()
			require.NoError(t, rows.Err())

			if bytes.Compare(original, buf) != 0 {
				return
			}
		}

		t.Fatal("expected buffer from RawValues to be overwritten by subsequent queries but it was not")
	})
}
