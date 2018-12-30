package pgx_test

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"
)

func TestCrateDBConnect(t *testing.T) {
	t.Parallel()

	if cratedbConnConfig == nil {
		t.Skip("Skipping due to undefined cratedbConnConfig")
	}

	conn, err := pgx.ConnectConfig(context.Background(), cratedbConnConfig)
	if err != nil {
		t.Fatalf("Unable to establish connection: %v", err)
	}

	var result int
	err = conn.QueryRow("select 1 +1").Scan(&result)
	if err != nil {
		t.Fatalf("QueryRow Scan unexpectedly failed: %v", err)
	}
	if result != 2 {
		t.Errorf("bad result: %d", result)
	}

	err = conn.Close()
	if err != nil {
		t.Fatal("Unable to close connection")
	}
}

func TestConnect(t *testing.T) {
	t.Parallel()

	conn, err := pgx.ConnectConfig(context.Background(), defaultConnConfig)
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
	if currentDB != defaultConnConfig.Config.Database {
		t.Errorf("Did not connect to specified database (%v)", defaultConnConfig.Config.Database)
	}

	var user string
	err = conn.QueryRow("select current_user").Scan(&user)
	if err != nil {
		t.Fatalf("QueryRow Scan unexpectedly failed: %v", err)
	}
	if user != defaultConnConfig.Config.User {
		t.Errorf("Did not connect as specified user (%v)", defaultConnConfig.Config.User)
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
		t.Skip("Skipping due to undefined unixSocketConnConfig")
	}

	conn, err := pgx.ConnectConfig(context.Background(), unixSocketConnConfig)
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
		t.Skip("Skipping due to undefined tcpConnConfig")
	}

	conn, err := pgx.ConnectConfig(context.Background(), tcpConnConfig)
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
		t.Skip("Skipping due to undefined tlsConnConfig")
	}

	conn, err := pgx.ConnectConfig(context.Background(), tlsConnConfig)
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
		t.Skip("Skipping due to undefined invalidUserConnConfig")
	}

	_, err := pgx.ConnectConfig(context.Background(), invalidUserConnConfig)
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
		t.Skip("Skipping due to undefined plainPasswordConnConfig")
	}

	conn, err := pgx.ConnectConfig(context.Background(), plainPasswordConnConfig)
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
		t.Skip("Skipping due to undefined md5ConnConfig")
	}

	conn, err := pgx.ConnectConfig(context.Background(), md5ConnConfig)
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

	_, err := pgx.ConnectConfig(context.Background(), &bad)
	if err == nil {
		t.Fatal("Expected error establishing connection to bad port")
	}
}

func TestConnectWithPreferSimpleProtocol(t *testing.T) {
	t.Parallel()

	connConfig := *defaultConnConfig
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

func TestConnectCustomDialer(t *testing.T) {
	t.Parallel()

	if customDialerConnConfig == nil {
		t.Skip("Skipping due to undefined customDialerConnConfig")
	}

	dialled := false
	conf := *customDialerConnConfig
	conf.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
		dialled = true
		return net.Dial(network, address)
	}

	conn, err := pgx.ConnectConfig(context.Background(), &conf)
	if err != nil {
		t.Fatalf("Unable to establish connection: %s", err)
	}
	if !dialled {
		t.Fatal("Connect did not use custom dialer")
	}

	err = conn.Close()
	if err != nil {
		t.Fatal("Unable to close connection")
	}
}

func TestConnectWithRuntimeParams(t *testing.T) {
	t.Parallel()

	connConfig := *defaultConnConfig
	connConfig.RuntimeParams = map[string]string{
		"application_name": "pgxtest",
		"search_path":      "myschema",
	}

	conn, err := pgx.ConnectConfig(context.Background(), &connConfig)
	if err != nil {
		t.Fatalf("Unable to establish connection: %v", err)
	}
	defer conn.Close()

	var s string
	err = conn.QueryRow("show application_name").Scan(&s)
	if err != nil {
		t.Fatalf("QueryRow Scan unexpectedly failed: %v", err)
	}
	if s != "pgxtest" {
		t.Errorf("Expected application_name to be %s, but it was %s", "pgxtest", s)
	}

	err = conn.QueryRow("show search_path").Scan(&s)
	if err != nil {
		t.Fatalf("QueryRow Scan unexpectedly failed: %v", err)
	}
	if s != "myschema" {
		t.Errorf("Expected search_path to be %s, but it was %s", "myschema", s)
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

	// Exec no-op which does not return a command tag
	if results := mustExec(t, conn, "--;"); results != "" {
		t.Errorf("Unexpected results from Exec: %v", results)
	}
}

func TestExecFailure(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	if _, err := conn.Exec("selct;"); err == nil {
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

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	if _, err := conn.Exec("selct $1;", 1); err == nil {
		t.Fatal("Expected SQL syntax error")
	}
	if conn.LastStmtSent() {
		t.Error("Expected LastStmtSent to return false")
	}
}

func TestExecExContextWithoutCancelation(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	commandTag, err := conn.ExecEx(ctx, "create temporary table foo(id integer primary key);", nil)
	if err != nil {
		t.Fatal(err)
	}
	if commandTag != "CREATE TABLE" {
		t.Fatalf("Unexpected results from ExecEx: %v", commandTag)
	}
	if !conn.LastStmtSent() {
		t.Error("Expected LastStmtSent to return true")
	}
}

func TestExecExContextFailureWithoutCancelation(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	if _, err := conn.ExecEx(ctx, "selct;", nil); err == nil {
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

func TestExecExContextFailureWithoutCancelationWithArguments(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	if _, err := conn.ExecEx(ctx, "selct $1;", nil, 1); err == nil {
		t.Fatal("Expected SQL syntax error")
	}
	if conn.LastStmtSent() {
		t.Error("Expected LastStmtSent to return false")
	}
}

func TestExecExContextCancelationCancelsQuery(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	ctx, cancelFunc := context.WithCancel(context.Background())
	go func() {
		time.Sleep(500 * time.Millisecond)
		cancelFunc()
	}()

	_, err := conn.ExecEx(ctx, "select pg_sleep(60)", nil)
	if err != context.Canceled {
		t.Fatalf("Expected context.Canceled err, got %v", err)
	}
	if !conn.LastStmtSent() {
		t.Error("Expected LastStmtSent to return true")
	}

	ensureConnValid(t, conn)
}

func TestExecFailureCloseBefore(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	closeConn(t, conn)

	if _, err := conn.Exec("select 1"); err == nil {
		t.Fatal("Expected network error")
	}
	if conn.LastStmtSent() {
		t.Error("Expected LastStmtSent to return false")
	}
}

func TestExecExExtendedProtocol(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	commandTag, err := conn.ExecEx(ctx, "create temporary table foo(name varchar primary key);", nil)
	if err != nil {
		t.Fatal(err)
	}
	if commandTag != "CREATE TABLE" {
		t.Fatalf("Unexpected results from ExecEx: %v", commandTag)
	}

	commandTag, err = conn.ExecEx(
		ctx,
		"insert into foo(name) values($1);",
		nil,
		"bar",
	)
	if err != nil {
		t.Fatal(err)
	}
	if commandTag != "INSERT 0 1" {
		t.Fatalf("Unexpected results from ExecEx: %v", commandTag)
	}

	ensureConnValid(t, conn)
}

func TestExecExSimpleProtocol(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	commandTag, err := conn.ExecEx(ctx, "create temporary table foo(name varchar primary key);", nil)
	if err != nil {
		t.Fatal(err)
	}
	if commandTag != "CREATE TABLE" {
		t.Fatalf("Unexpected results from ExecEx: %v", commandTag)
	}
	if !conn.LastStmtSent() {
		t.Error("Expected LastStmtSent to return true")
	}

	commandTag, err = conn.ExecEx(
		ctx,
		"insert into foo(name) values($1);",
		&pgx.QueryExOptions{SimpleProtocol: true},
		"bar'; drop table foo;--",
	)
	if err != nil {
		t.Fatal(err)
	}
	if commandTag != "INSERT 0 1" {
		t.Fatalf("Unexpected results from ExecEx: %v", commandTag)
	}
	if !conn.LastStmtSent() {
		t.Error("Expected LastStmtSent to return true")
	}
}

func TestConnExecExSuppliedCorrectParameterOIDs(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	mustExec(t, conn, "create temporary table foo(name varchar primary key);")

	commandTag, err := conn.ExecEx(
		context.Background(),
		"insert into foo(name) values($1);",
		&pgx.QueryExOptions{ParameterOIDs: []pgtype.OID{pgtype.VarcharOID}},
		"bar'; drop table foo;--",
	)
	if err != nil {
		t.Fatal(err)
	}
	if commandTag != "INSERT 0 1" {
		t.Fatalf("Unexpected results from ExecEx: %v", commandTag)
	}
	if !conn.LastStmtSent() {
		t.Error("Expected LastStmtSent to return true")
	}
}

func TestConnExecExSuppliedIncorrectParameterOIDs(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	mustExec(t, conn, "create temporary table foo(name varchar primary key);")

	_, err := conn.ExecEx(
		context.Background(),
		"insert into foo(name) values($1);",
		&pgx.QueryExOptions{ParameterOIDs: []pgtype.OID{pgtype.Int4OID}},
		"bar'; drop table foo;--",
	)
	if err == nil {
		t.Fatal("expected error but got none")
	}
	if !conn.LastStmtSent() {
		t.Error("Expected LastStmtSent to return true")
	}
}

func TestConnExecExIncorrectParameterOIDsAfterAnotherQuery(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	mustExec(t, conn, "create temporary table foo(name varchar primary key);")

	var s string
	err := conn.QueryRow("insert into foo(name) values('baz') returning name;").Scan(&s)
	if err != nil {
		t.Errorf("Executing query failed: %v", err)
	}
	if s != "baz" {
		t.Errorf("Query did not return expected value: %v", s)
	}

	_, err = conn.ExecEx(
		context.Background(),
		"insert into foo(name) values($1);",
		&pgx.QueryExOptions{ParameterOIDs: []pgtype.OID{pgtype.Int4OID}},
		"bar'; drop table foo;--",
	)
	if err == nil {
		t.Fatal("expected error but got none")
	}
	if !conn.LastStmtSent() {
		t.Error("Expected LastStmtSent to return true")
	}
}

func TestExecExFailureCloseBefore(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	closeConn(t, conn)

	if _, err := conn.ExecEx(context.Background(), "select 1", nil); err == nil {
		t.Fatal("Expected network error")
	}
	if conn.LastStmtSent() {
		t.Error("Expected LastStmtSent to return false")
	}
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

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	if _, err := conn.Prepare("badSQL", "select foo"); err == nil {
		t.Fatal("Prepare should have failed with syntax error")
	}

	ensureConnValid(t, conn)
}

func TestPrepareQueryManyParameters(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
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

	conn := mustConnect(t, *defaultConnConfig)
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

	conn := mustConnect(t, *defaultConnConfig)
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
	notification, err := listener.WaitForNotification(context.Background())
	if err != nil {
		t.Fatalf("Unexpected error on WaitForNotification: %v", err)
	}
	if notification.Channel != "chat" {
		t.Errorf("Did not receive notification on expected channel: %v", notification.Channel)
	}

	// when notification has already been read during previous query
	mustExec(t, notifier, "notify chat")
	rows, _ := listener.Query("select 1")
	rows.Close()
	if rows.Err() != nil {
		t.Fatalf("Unexpected error on Query: %v", rows.Err())
	}

	ctx, cancelFn := context.WithCancel(context.Background())
	cancelFn()
	notification, err = listener.WaitForNotification(ctx)
	if err != nil {
		t.Fatalf("Unexpected error on WaitForNotification: %v", err)
	}
	if notification.Channel != "chat" {
		t.Errorf("Did not receive notification on expected channel: %v", notification.Channel)
	}

	// when timeout occurs
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	notification, err = listener.WaitForNotification(ctx)
	if err != context.DeadlineExceeded {
		t.Errorf("WaitForNotification returned the wrong kind of error: %v", err)
	}
	if notification != nil {
		t.Errorf("WaitForNotification returned an unexpected notification: %v", notification)
	}

	// listener can listen again after a timeout
	mustExec(t, notifier, "notify chat")
	notification, err = listener.WaitForNotification(context.Background())
	if err != nil {
		t.Fatalf("Unexpected error on WaitForNotification: %v", err)
	}
	if notification.Channel != "chat" {
		t.Errorf("Did not receive notification on expected channel: %v", notification.Channel)
	}
}

func TestUnlistenSpecificChannel(t *testing.T) {
	t.Parallel()

	listener := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, listener)

	if err := listener.Listen("unlisten_test"); err != nil {
		t.Fatalf("Unable to start listening: %v", err)
	}

	notifier := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, notifier)

	mustExec(t, notifier, "notify unlisten_test")

	// when notification is waiting on the socket to be read
	notification, err := listener.WaitForNotification(context.Background())
	if err != nil {
		t.Fatalf("Unexpected error on WaitForNotification: %v", err)
	}
	if notification.Channel != "unlisten_test" {
		t.Errorf("Did not receive notification on expected channel: %v", notification.Channel)
	}

	err = listener.Unlisten("unlisten_test")
	if err != nil {
		t.Fatalf("Unexpected error on Unlisten: %v", err)
	}

	// when notification has already been read during previous query
	mustExec(t, notifier, "notify unlisten_test")
	rows, _ := listener.Query("select 1")
	rows.Close()
	if rows.Err() != nil {
		t.Fatalf("Unexpected error on Query: %v", rows.Err())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	notification, err = listener.WaitForNotification(ctx)
	if err != context.DeadlineExceeded {
		t.Errorf("WaitForNotification returned the wrong kind of error: %v", err)
	}
}

func TestListenNotifyWhileBusyIsSafe(t *testing.T) {
	t.Parallel()

	listenerDone := make(chan bool)
	go func() {
		conn := mustConnect(t, *defaultConnConfig)
		defer closeConn(t, conn)
		defer func() {
			listenerDone <- true
		}()

		if err := conn.Listen("busysafe"); err != nil {
			t.Fatalf("Unable to start listening: %v", err)
		}

		for i := 0; i < 5000; i++ {
			var sum int32
			var rowCount int32

			rows, err := conn.Query("select generate_series(1,$1)", 100)
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
		conn := mustConnect(t, *defaultConnConfig)
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

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	if err := conn.Listen("self"); err != nil {
		t.Fatalf("Unable to start listening: %v", err)
	}

	// Notify self and WaitForNotification immediately
	mustExec(t, conn, "notify self")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	notification, err := conn.WaitForNotification(ctx)
	if err != nil {
		t.Fatalf("Unexpected error on WaitForNotification: %v", err)
	}
	if notification.Channel != "self" {
		t.Errorf("Did not receive notification on expected channel: %v", notification.Channel)
	}

	// Notify self and do something else before WaitForNotification
	mustExec(t, conn, "notify self")

	rows, _ := conn.Query("select 1")
	rows.Close()
	if rows.Err() != nil {
		t.Fatalf("Unexpected error on Query: %v", rows.Err())
	}

	ctx, cncl := context.WithTimeout(context.Background(), time.Second)
	defer cncl()
	notification, err = conn.WaitForNotification(ctx)
	if err != nil {
		t.Fatalf("Unexpected error on WaitForNotification: %v", err)
	}
	if notification.Channel != "self" {
		t.Errorf("Did not receive notification on expected channel: %v", notification.Channel)
	}
}

func TestListenUnlistenSpecialCharacters(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	chanName := "special characters !@#{$%^&*()}"
	if err := conn.Listen(chanName); err != nil {
		t.Fatalf("Unable to start listening: %v", err)
	}

	if err := conn.Unlisten(chanName); err != nil {
		t.Fatalf("Unable to stop listening: %v", err)
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
		if err == pgx.ErrDeadConn {
		} else if pgErr, ok := err.(pgx.PgError); ok && pgErr.Severity == "FATAL" {
		} else {
			t.Fatalf("Expected QueryRow Scan to return fatal PgError or ErrDeadConn, but instead received %v", err)
		}
	}()

	otherConn, err := pgx.ConnectConfig(context.Background(), defaultConnConfig)
	if err != nil {
		t.Fatalf("Unable to establish connection: %v", err)
	}
	defer otherConn.Close()

	if _, err := otherConn.Exec("select pg_terminate_backend($1)", conn.PID()); err != nil {
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
			conn := mustConnect(t, *defaultConnConfig)
			defer closeConn(t, conn)

			otherConn, err := pgx.ConnectConfig(context.Background(), defaultConnConfig)
			if err != nil {
				t.Fatalf("Unable to establish connection: %v", err)
			}
			defer otherConn.Close()

			_, err = otherConn.Exec("select pg_terminate_backend($1)", conn.PID())
			if err != nil {
				t.Fatalf("Unable to kill backend PostgreSQL process: %v", err)
			}

			_, err = conn.Query("select 1")
			if err == nil {
				t.Fatal("Expected error but none occurred")
			}

			if conn.IsAlive() {
				t.Fatalf("Connection should not be live but was. Previous Query err: %v", err)
			}
		}()
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

func TestInsertBoolArray(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	if results := mustExec(t, conn, "create temporary table foo(spice bool[]);"); results != "CREATE TABLE" {
		t.Error("Unexpected results from Exec")
	}

	// Accept parameters
	if results := mustExec(t, conn, "insert into foo(spice) values($1)", []bool{true, false, true}); results != "INSERT 0 1" {
		t.Errorf("Unexpected results from Exec: %v", results)
	}
}

func TestInsertTimestampArray(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	if results := mustExec(t, conn, "create temporary table foo(spice timestamp[]);"); results != "CREATE TABLE" {
		t.Error("Unexpected results from Exec")
	}

	// Accept parameters
	if results := mustExec(t, conn, "insert into foo(spice) values($1)", []time.Time{time.Unix(1419143667, 0), time.Unix(1419143672, 0)}); results != "INSERT 0 1" {
		t.Errorf("Unexpected results from Exec: %v", results)
	}
}

func TestCatchSimultaneousConnectionQueries(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
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

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	rows, err := conn.Query("select generate_series(1,$1)", 10)
	if err != nil {
		t.Fatalf("conn.Query failed: %v", err)
	}
	defer rows.Close()

	_, err = conn.Exec("create temporary table foo(spice timestamp[])")
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

	conn := mustConnect(t, *defaultConnConfig)
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

	conn := mustConnect(t, *defaultConnConfig)
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

func TestConnOnNotice(t *testing.T) {
	t.Parallel()

	var msg string

	connConfig := *defaultConnConfig
	connConfig.OnNotice = func(c *pgx.Conn, notice *pgx.Notice) {
		msg = notice.Message
	}
	conn := mustConnect(t, connConfig)
	defer closeConn(t, conn)

	_, err := conn.Exec(`do $$
begin
  raise notice 'hello, world';
end$$;`)
	if err != nil {
		t.Fatal(err)
	}

	if msg != "hello, world" {
		t.Errorf("msg => %v, want %v", msg, "hello, world")
	}

	ensureConnValid(t, conn)
}

func TestConnInitConnInfo(t *testing.T) {
	conn := mustConnect(t, *defaultConnConfig)
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
	conn := mustConnect(t, *defaultConnConfig)
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
