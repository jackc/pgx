package pgx_test

import (
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

	rows, _ := conn.Query("select 1")
	rows.Close()
	if rows.Err() != nil {
		t.Fatalf("Exec failure appears to have broken connection: %v", rows.Err())
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
	rows, _ := listener.Query("select 1")
	rows.Close()
	if rows.Err() != nil {
		t.Fatalf("Unexpected error on Query: %v", rows.Err())
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
