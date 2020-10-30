package pgx_test

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgconn/stmtcache"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnSendBatch(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	sql := `create temporary table ledger(
	  id serial primary key,
	  description varchar not null,
	  amount int not null
	);`
	mustExec(t, conn, sql)

	batch := &pgx.Batch{}
	batch.Queue("insert into ledger(description, amount) values($1, $2)", "q1", 1)
	batch.Queue("insert into ledger(description, amount) values($1, $2)", "q2", 2)
	batch.Queue("insert into ledger(description, amount) values($1, $2)", "q3", 3)
	batch.Queue("select id, description, amount from ledger order by id")
	batch.Queue("select sum(amount) from ledger")

	br := conn.SendBatch(context.Background(), batch)

	ct, err := br.Exec()
	if err != nil {
		t.Error(err)
	}
	if ct.RowsAffected() != 1 {
		t.Errorf("ct.RowsAffected() => %v, want %v", ct.RowsAffected(), 1)
	}

	ct, err = br.Exec()
	if err != nil {
		t.Error(err)
	}
	if ct.RowsAffected() != 1 {
		t.Errorf("ct.RowsAffected() => %v, want %v", ct.RowsAffected(), 1)
	}

	ct, err = br.Exec()
	if err != nil {
		t.Error(err)
	}
	if ct.RowsAffected() != 1 {
		t.Errorf("ct.RowsAffected() => %v, want %v", ct.RowsAffected(), 1)
	}

	rows, err := br.Query()
	if err != nil {
		t.Error(err)
	}

	var id int32
	var description string
	var amount int32
	if !rows.Next() {
		t.Fatal("expected a row to be available")
	}
	if err := rows.Scan(&id, &description, &amount); err != nil {
		t.Fatal(err)
	}
	if id != 1 {
		t.Errorf("id => %v, want %v", id, 1)
	}
	if description != "q1" {
		t.Errorf("description => %v, want %v", description, "q1")
	}
	if amount != 1 {
		t.Errorf("amount => %v, want %v", amount, 1)
	}

	if !rows.Next() {
		t.Fatal("expected a row to be available")
	}
	if err := rows.Scan(&id, &description, &amount); err != nil {
		t.Fatal(err)
	}
	if id != 2 {
		t.Errorf("id => %v, want %v", id, 2)
	}
	if description != "q2" {
		t.Errorf("description => %v, want %v", description, "q2")
	}
	if amount != 2 {
		t.Errorf("amount => %v, want %v", amount, 2)
	}

	if !rows.Next() {
		t.Fatal("expected a row to be available")
	}
	if err := rows.Scan(&id, &description, &amount); err != nil {
		t.Fatal(err)
	}
	if id != 3 {
		t.Errorf("id => %v, want %v", id, 3)
	}
	if description != "q3" {
		t.Errorf("description => %v, want %v", description, "q3")
	}
	if amount != 3 {
		t.Errorf("amount => %v, want %v", amount, 3)
	}

	if rows.Next() {
		t.Fatal("did not expect a row to be available")
	}

	if rows.Err() != nil {
		t.Fatal(rows.Err())
	}

	err = br.QueryRow().Scan(&amount)
	if err != nil {
		t.Error(err)
	}
	if amount != 6 {
		t.Errorf("amount => %v, want %v", amount, 6)
	}

	err = br.Close()
	if err != nil {
		t.Fatal(err)
	}

	ensureConnValid(t, conn)
}

func TestConnSendBatchMany(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	sql := `create temporary table ledger(
	  id serial primary key,
	  description varchar not null,
	  amount int not null
	);`
	mustExec(t, conn, sql)

	batch := &pgx.Batch{}

	numInserts := 1000

	for i := 0; i < numInserts; i++ {
		batch.Queue("insert into ledger(description, amount) values($1, $2)", "q1", 1)
	}
	batch.Queue("select count(*) from ledger")

	br := conn.SendBatch(context.Background(), batch)

	for i := 0; i < numInserts; i++ {
		ct, err := br.Exec()
		assert.NoError(t, err)
		assert.EqualValues(t, 1, ct.RowsAffected())
	}

	var actualInserts int
	err := br.QueryRow().Scan(&actualInserts)
	assert.NoError(t, err)
	assert.EqualValues(t, numInserts, actualInserts)

	err = br.Close()
	require.NoError(t, err)

	ensureConnValid(t, conn)
}

func TestConnSendBatchWithPreparedStatement(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	_, err := conn.Prepare(context.Background(), "ps1", "select n from generate_series(0,$1::int) n")
	if err != nil {
		t.Fatal(err)
	}

	batch := &pgx.Batch{}

	queryCount := 3
	for i := 0; i < queryCount; i++ {
		batch.Queue("ps1", 5)
	}

	br := conn.SendBatch(context.Background(), batch)

	for i := 0; i < queryCount; i++ {
		rows, err := br.Query()
		if err != nil {
			t.Fatal(err)
		}

		for k := 0; rows.Next(); k++ {
			var n int
			if err := rows.Scan(&n); err != nil {
				t.Fatal(err)
			}
			if n != k {
				t.Fatalf("n => %v, want %v", n, k)
			}
		}

		if rows.Err() != nil {
			t.Fatal(rows.Err())
		}
	}

	err = br.Close()
	if err != nil {
		t.Fatal(err)
	}

	ensureConnValid(t, conn)
}

// https://github.com/jackc/pgx/issues/856
func TestConnSendBatchWithPreparedStatementAndStatementCacheDisabled(t *testing.T) {
	t.Parallel()

	config, err := pgx.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)

	config.BuildStatementCache = nil

	conn := mustConnect(t, config)
	defer closeConn(t, conn)

	_, err = conn.Prepare(context.Background(), "ps1", "select n from generate_series(0,$1::int) n")
	if err != nil {
		t.Fatal(err)
	}

	batch := &pgx.Batch{}

	queryCount := 3
	for i := 0; i < queryCount; i++ {
		batch.Queue("ps1", 5)
	}

	br := conn.SendBatch(context.Background(), batch)

	for i := 0; i < queryCount; i++ {
		rows, err := br.Query()
		if err != nil {
			t.Fatal(err)
		}

		for k := 0; rows.Next(); k++ {
			var n int
			if err := rows.Scan(&n); err != nil {
				t.Fatal(err)
			}
			if n != k {
				t.Fatalf("n => %v, want %v", n, k)
			}
		}

		if rows.Err() != nil {
			t.Fatal(rows.Err())
		}
	}

	err = br.Close()
	if err != nil {
		t.Fatal(err)
	}

	ensureConnValid(t, conn)
}

func TestConnSendBatchCloseRowsPartiallyRead(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	batch := &pgx.Batch{}
	batch.Queue("select n from generate_series(0,5) n")
	batch.Queue("select n from generate_series(0,5) n")

	br := conn.SendBatch(context.Background(), batch)

	rows, err := br.Query()
	if err != nil {
		t.Error(err)
	}

	for i := 0; i < 3; i++ {
		if !rows.Next() {
			t.Error("expected a row to be available")
		}

		var n int
		if err := rows.Scan(&n); err != nil {
			t.Error(err)
		}
		if n != i {
			t.Errorf("n => %v, want %v", n, i)
		}
	}

	rows.Close()

	rows, err = br.Query()
	if err != nil {
		t.Error(err)
	}

	for i := 0; rows.Next(); i++ {
		var n int
		if err := rows.Scan(&n); err != nil {
			t.Error(err)
		}
		if n != i {
			t.Errorf("n => %v, want %v", n, i)
		}
	}

	if rows.Err() != nil {
		t.Error(rows.Err())
	}

	err = br.Close()
	if err != nil {
		t.Fatal(err)
	}

	ensureConnValid(t, conn)
}

func TestConnSendBatchQueryError(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	batch := &pgx.Batch{}
	batch.Queue("select n from generate_series(0,5) n where 100/(5-n) > 0")
	batch.Queue("select n from generate_series(0,5) n")

	br := conn.SendBatch(context.Background(), batch)

	rows, err := br.Query()
	if err != nil {
		t.Error(err)
	}

	for i := 0; rows.Next(); i++ {
		var n int
		if err := rows.Scan(&n); err != nil {
			t.Error(err)
		}
		if n != i {
			t.Errorf("n => %v, want %v", n, i)
		}
	}

	if pgErr, ok := rows.Err().(*pgconn.PgError); !(ok && pgErr.Code == "22012") {
		t.Errorf("rows.Err() => %v, want error code %v", rows.Err(), 22012)
	}

	err = br.Close()
	if pgErr, ok := err.(*pgconn.PgError); !(ok && pgErr.Code == "22012") {
		t.Errorf("rows.Err() => %v, want error code %v", err, 22012)
	}

	ensureConnValid(t, conn)
}

func TestConnSendBatchQuerySyntaxError(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	batch := &pgx.Batch{}
	batch.Queue("select 1 1")

	br := conn.SendBatch(context.Background(), batch)

	var n int32
	err := br.QueryRow().Scan(&n)
	if pgErr, ok := err.(*pgconn.PgError); !(ok && pgErr.Code == "42601") {
		t.Errorf("rows.Err() => %v, want error code %v", err, 42601)
	}

	err = br.Close()
	if err == nil {
		t.Error("Expected error")
	}

	ensureConnValid(t, conn)
}

func TestConnSendBatchQueryRowInsert(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	sql := `create temporary table ledger(
	  id serial primary key,
	  description varchar not null,
	  amount int not null
	);`
	mustExec(t, conn, sql)

	batch := &pgx.Batch{}
	batch.Queue("select 1")
	batch.Queue("insert into ledger(description, amount) values($1, $2),($1, $2)", "q1", 1)

	br := conn.SendBatch(context.Background(), batch)

	var value int
	err := br.QueryRow().Scan(&value)
	if err != nil {
		t.Error(err)
	}

	ct, err := br.Exec()
	if err != nil {
		t.Error(err)
	}
	if ct.RowsAffected() != 2 {
		t.Errorf("ct.RowsAffected() => %v, want %v", ct.RowsAffected(), 2)
	}

	br.Close()

	ensureConnValid(t, conn)
}

func TestConnSendBatchQueryPartialReadInsert(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	sql := `create temporary table ledger(
	  id serial primary key,
	  description varchar not null,
	  amount int not null
	);`
	mustExec(t, conn, sql)

	batch := &pgx.Batch{}
	batch.Queue("select 1 union all select 2 union all select 3")
	batch.Queue("insert into ledger(description, amount) values($1, $2),($1, $2)", "q1", 1)

	br := conn.SendBatch(context.Background(), batch)

	rows, err := br.Query()
	if err != nil {
		t.Error(err)
	}
	rows.Close()

	ct, err := br.Exec()
	if err != nil {
		t.Error(err)
	}
	if ct.RowsAffected() != 2 {
		t.Errorf("ct.RowsAffected() => %v, want %v", ct.RowsAffected(), 2)
	}

	br.Close()

	ensureConnValid(t, conn)
}

func TestTxSendBatch(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	sql := `create temporary table ledger1(
	  id serial primary key,
	  description varchar not null
	);`
	mustExec(t, conn, sql)

	sql = `create temporary table ledger2(
	  id int primary key,
	  amount int not null
	);`
	mustExec(t, conn, sql)

	tx, _ := conn.Begin(context.Background())
	batch := &pgx.Batch{}
	batch.Queue("insert into ledger1(description) values($1) returning id", "q1")

	br := tx.SendBatch(context.Background(), batch)

	var id int
	err := br.QueryRow().Scan(&id)
	if err != nil {
		t.Error(err)
	}
	br.Close()

	batch = &pgx.Batch{}
	batch.Queue("insert into ledger2(id,amount) values($1, $2)", id, 2)
	batch.Queue("select amount from ledger2 where id = $1", id)

	br = tx.SendBatch(context.Background(), batch)

	ct, err := br.Exec()
	if err != nil {
		t.Error(err)
	}
	if ct.RowsAffected() != 1 {
		t.Errorf("ct.RowsAffected() => %v, want %v", ct.RowsAffected(), 1)
	}

	var amount int
	err = br.QueryRow().Scan(&amount)
	if err != nil {
		t.Error(err)
	}

	br.Close()
	tx.Commit(context.Background())

	var count int
	conn.QueryRow(context.Background(), "select count(1) from ledger1 where id = $1", id).Scan(&count)
	if count != 1 {
		t.Errorf("count => %v, want %v", count, 1)
	}

	err = br.Close()
	if err != nil {
		t.Fatal(err)
	}

	ensureConnValid(t, conn)
}

func TestTxSendBatchRollback(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	sql := `create temporary table ledger1(
	  id serial primary key,
	  description varchar not null
	);`
	mustExec(t, conn, sql)

	tx, _ := conn.Begin(context.Background())
	batch := &pgx.Batch{}
	batch.Queue("insert into ledger1(description) values($1) returning id", "q1")

	br := tx.SendBatch(context.Background(), batch)

	var id int
	err := br.QueryRow().Scan(&id)
	if err != nil {
		t.Error(err)
	}
	br.Close()
	tx.Rollback(context.Background())

	row := conn.QueryRow(context.Background(), "select count(1) from ledger1 where id = $1", id)
	var count int
	row.Scan(&count)
	if count != 0 {
		t.Errorf("count => %v, want %v", count, 0)
	}

	ensureConnValid(t, conn)
}

func TestConnBeginBatchDeferredError(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	mustExec(t, conn, `create temporary table t (
		id text primary key,
		n int not null,
		unique (n) deferrable initially deferred
	);

	insert into t (id, n) values ('a', 1), ('b', 2), ('c', 3);`)

	batch := &pgx.Batch{}

	batch.Queue(`update t set n=n+1 where id='b' returning *`)

	br := conn.SendBatch(context.Background(), batch)

	rows, err := br.Query()
	if err != nil {
		t.Error(err)
	}

	for rows.Next() {
		var id string
		var n int32
		err = rows.Scan(&id, &n)
		if err != nil {
			t.Fatal(err)
		}
	}

	err = br.Close()
	if err == nil {
		t.Fatal("expected error 23505 but got none")
	}

	if err, ok := err.(*pgconn.PgError); !ok || err.Code != "23505" {
		t.Fatalf("expected error 23505, got %v", err)
	}

	ensureConnValid(t, conn)
}

func TestConnSendBatchNoStatementCache(t *testing.T) {
	config := mustParseConfig(t, os.Getenv("PGX_TEST_DATABASE"))
	config.BuildStatementCache = nil

	conn := mustConnect(t, config)
	defer closeConn(t, conn)

	testConnSendBatch(t, conn, 3)
}

func TestConnSendBatchPrepareStatementCache(t *testing.T) {
	config := mustParseConfig(t, os.Getenv("PGX_TEST_DATABASE"))
	config.BuildStatementCache = func(conn *pgconn.PgConn) stmtcache.Cache {
		return stmtcache.New(conn, stmtcache.ModePrepare, 32)
	}

	conn := mustConnect(t, config)
	defer closeConn(t, conn)

	testConnSendBatch(t, conn, 3)
}

func TestConnSendBatchDescribeStatementCache(t *testing.T) {
	config := mustParseConfig(t, os.Getenv("PGX_TEST_DATABASE"))
	config.BuildStatementCache = func(conn *pgconn.PgConn) stmtcache.Cache {
		return stmtcache.New(conn, stmtcache.ModeDescribe, 32)
	}

	conn := mustConnect(t, config)
	defer closeConn(t, conn)

	testConnSendBatch(t, conn, 3)
}

func testConnSendBatch(t *testing.T, conn *pgx.Conn, queryCount int) {
	batch := &pgx.Batch{}
	for j := 0; j < queryCount; j++ {
		batch.Queue("select n from generate_series(0,5) n")
	}

	br := conn.SendBatch(context.Background(), batch)

	for j := 0; j < queryCount; j++ {
		rows, err := br.Query()
		require.NoError(t, err)

		for k := 0; rows.Next(); k++ {
			var n int
			err := rows.Scan(&n)
			require.NoError(t, err)
			require.Equal(t, k, n)
		}

		require.NoError(t, rows.Err())
	}

	err := br.Close()
	require.NoError(t, err)
}

func TestLogBatchStatementsOnExec(t *testing.T) {
	l1 := &testLogger{}
	config := mustParseConfig(t, os.Getenv("PGX_TEST_DATABASE"))
	config.Logger = l1

	conn := mustConnect(t, config)
	defer closeConn(t, conn)

	l1.logs = l1.logs[0:0] // Clear logs written when establishing connection

	batch := &pgx.Batch{}
	batch.Queue("create table foo (id bigint)")
	batch.Queue("drop table foo")

	br := conn.SendBatch(context.Background(), batch)

	_, err := br.Exec()
	if err != nil {
		t.Fatalf("Unexpected error creating table: %v", err)
	}

	_, err = br.Exec()
	if err != nil {
		t.Fatalf("Unexpected error dropping table: %v", err)
	}

	if len(l1.logs) != 2 {
		t.Fatalf("Expected two log entries but got %d", len(l1.logs))
	}

	if l1.logs[0].msg != "BatchResult.Exec" {
		t.Errorf("Expected first log message to be 'BatchResult.Exec' but was '%s", l1.logs[0].msg)
	}

	if l1.logs[0].data["sql"] != "create table foo (id bigint)" {
		t.Errorf("Expected the first query to be 'create table foo (id bigint)' but was '%s'", l1.logs[0].data["sql"])
	}

	if l1.logs[1].msg != "BatchResult.Exec" {
		t.Errorf("Expected second log message to be 'BatchResult.Exec' but was '%s", l1.logs[1].msg)
	}

	if l1.logs[1].data["sql"] != "drop table foo" {
		t.Errorf("Expected the second query to be 'drop table foo' but was '%s'", l1.logs[1].data["sql"])
	}
}

func TestLogBatchStatementsOnBatchResultClose(t *testing.T) {
	l1 := &testLogger{}
	config := mustParseConfig(t, os.Getenv("PGX_TEST_DATABASE"))
	config.Logger = l1

	conn := mustConnect(t, config)
	defer closeConn(t, conn)

	l1.logs = l1.logs[0:0] // Clear logs written when establishing connection

	batch := &pgx.Batch{}
	batch.Queue("select generate_series(1,$1)", 100)
	batch.Queue("select 1 = 1;")

	br := conn.SendBatch(context.Background(), batch)

	if err := br.Close(); err != nil {
		t.Fatalf("Unexpected batch error: %v", err)
	}

	if len(l1.logs) != 2 {
		t.Fatalf("Expected 2 log statements but found %d", len(l1.logs))
	}

	if l1.logs[0].msg != "BatchResult.Close" {
		t.Errorf("Expected first log statement to be 'BatchResult.Close' but was %s", l1.logs[0].msg)
	}

	if l1.logs[0].data["sql"] != "select generate_series(1,$1)" {
		t.Errorf("Expected first query to be 'select generate_series(1,$1)' but was '%s'", l1.logs[0].data["sql"])
	}

	if l1.logs[1].msg != "BatchResult.Close" {
		t.Errorf("Expected second log statement to be 'BatchResult.Close' but was %s", l1.logs[1].msg)
	}

	if l1.logs[1].data["sql"] != "select 1 = 1;" {
		t.Errorf("Expected second query to be 'select 1 = 1;' but was '%s'", l1.logs[1].data["sql"])
	}
}

func TestSendBatchSimpleProtocol(t *testing.T) {
	t.Parallel()

	config := mustParseConfig(t, os.Getenv("PGX_TEST_DATABASE"))
	config.PreferSimpleProtocol = true

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	conn := mustConnect(t, config)
	defer closeConn(t, conn)

	var batch pgx.Batch
	batch.Queue("SELECT 1::int")
	batch.Queue("SELECT 2::int; SELECT $1::int", 3)
	results := conn.SendBatch(ctx, &batch)
	rows, err := results.Query()
	assert.NoError(t, err)
	assert.True(t, rows.Next())
	values, err := rows.Values()
	assert.NoError(t, err)
	assert.Equal(t, int32(1), values[0])
	assert.False(t, rows.Next())

	rows, err = results.Query()
	assert.NoError(t, err)
	assert.True(t, rows.Next())
	values, err = rows.Values()
	assert.NoError(t, err)
	assert.Equal(t, int32(2), values[0])
	assert.False(t, rows.Next())

	rows, err = results.Query()
	assert.NoError(t, err)
	assert.True(t, rows.Next())
	values, err = rows.Values()
	assert.NoError(t, err)
	assert.Equal(t, int32(3), values[0])
	assert.False(t, rows.Next())
}
