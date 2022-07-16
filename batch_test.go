package pgx_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnSendBatch(t *testing.T) {
	t.Parallel()

	pgxtest.RunWithQueryExecModes(context.Background(), t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		pgxtest.SkipCockroachDB(t, conn, "Server serial type is incompatible with test")

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
		batch.Queue("select id, description, amount from ledger order by id")
		batch.Queue("select * from ledger where false")
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

		selectFromLedgerExpectedRows := []struct {
			id          int32
			description string
			amount      int32
		}{
			{1, "q1", 1},
			{2, "q2", 2},
			{3, "q3", 3},
		}

		rows, err := br.Query()
		if err != nil {
			t.Error(err)
		}

		var id int32
		var description string
		var amount int32
		rowCount := 0

		for rows.Next() {
			if rowCount >= len(selectFromLedgerExpectedRows) {
				t.Fatalf("got too many rows: %d", rowCount)
			}

			if err := rows.Scan(&id, &description, &amount); err != nil {
				t.Fatalf("row %d: %v", rowCount, err)
			}

			if id != selectFromLedgerExpectedRows[rowCount].id {
				t.Errorf("id => %v, want %v", id, selectFromLedgerExpectedRows[rowCount].id)
			}
			if description != selectFromLedgerExpectedRows[rowCount].description {
				t.Errorf("description => %v, want %v", description, selectFromLedgerExpectedRows[rowCount].description)
			}
			if amount != selectFromLedgerExpectedRows[rowCount].amount {
				t.Errorf("amount => %v, want %v", amount, selectFromLedgerExpectedRows[rowCount].amount)
			}

			rowCount++
		}

		if rows.Err() != nil {
			t.Fatal(rows.Err())
		}

		rowCount = 0
		rows, _ = br.Query()
		_, err = pgx.ForEachRow(rows, []any{&id, &description, &amount}, func() error {
			if id != selectFromLedgerExpectedRows[rowCount].id {
				t.Errorf("id => %v, want %v", id, selectFromLedgerExpectedRows[rowCount].id)
			}
			if description != selectFromLedgerExpectedRows[rowCount].description {
				t.Errorf("description => %v, want %v", description, selectFromLedgerExpectedRows[rowCount].description)
			}
			if amount != selectFromLedgerExpectedRows[rowCount].amount {
				t.Errorf("amount => %v, want %v", amount, selectFromLedgerExpectedRows[rowCount].amount)
			}

			rowCount++

			return nil
		})
		if err != nil {
			t.Error(err)
		}

		err = br.QueryRow().Scan(&id, &description, &amount)
		if !errors.Is(err, pgx.ErrNoRows) {
			t.Errorf("expected pgx.ErrNoRows but got: %v", err)
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
	})
}

func TestConnSendBatchQueuedQuery(t *testing.T) {
	t.Parallel()

	pgxtest.RunWithQueryExecModes(context.Background(), t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		pgxtest.SkipCockroachDB(t, conn, "Server serial type is incompatible with test")

		sql := `create temporary table ledger(
	  id serial primary key,
	  description varchar not null,
	  amount int not null
	);`
		mustExec(t, conn, sql)

		batch := &pgx.Batch{}

		batch.Queue("insert into ledger(description, amount) values($1, $2)", "q1", 1).Exec(func(ct pgconn.CommandTag) error {
			assert.EqualValues(t, 1, ct.RowsAffected())
			return nil
		})

		batch.Queue("insert into ledger(description, amount) values($1, $2)", "q2", 2).Exec(func(ct pgconn.CommandTag) error {
			assert.EqualValues(t, 1, ct.RowsAffected())
			return nil
		})

		batch.Queue("insert into ledger(description, amount) values($1, $2)", "q3", 3).Exec(func(ct pgconn.CommandTag) error {
			assert.EqualValues(t, 1, ct.RowsAffected())
			return nil
		})

		selectFromLedgerExpectedRows := []struct {
			id          int32
			description string
			amount      int32
		}{
			{1, "q1", 1},
			{2, "q2", 2},
			{3, "q3", 3},
		}

		batch.Queue("select id, description, amount from ledger order by id").Query(func(rows pgx.Rows) error {
			rowCount := 0
			var id int32
			var description string
			var amount int32
			_, err := pgx.ForEachRow(rows, []any{&id, &description, &amount}, func() error {
				assert.Equal(t, selectFromLedgerExpectedRows[rowCount].id, id)
				assert.Equal(t, selectFromLedgerExpectedRows[rowCount].description, description)
				assert.Equal(t, selectFromLedgerExpectedRows[rowCount].amount, amount)
				rowCount++

				return nil
			})
			assert.NoError(t, err)
			return nil
		})

		batch.Queue("select id, description, amount from ledger order by id").Query(func(rows pgx.Rows) error {
			rowCount := 0
			var id int32
			var description string
			var amount int32
			_, err := pgx.ForEachRow(rows, []any{&id, &description, &amount}, func() error {
				assert.Equal(t, selectFromLedgerExpectedRows[rowCount].id, id)
				assert.Equal(t, selectFromLedgerExpectedRows[rowCount].description, description)
				assert.Equal(t, selectFromLedgerExpectedRows[rowCount].amount, amount)
				rowCount++

				return nil
			})
			assert.NoError(t, err)
			return nil
		})

		batch.Queue("select * from ledger where false").QueryRow(func(row pgx.Row) error {
			err := row.Scan(nil, nil, nil)
			assert.ErrorIs(t, err, pgx.ErrNoRows)
			return nil
		})

		batch.Queue("select sum(amount) from ledger").QueryRow(func(row pgx.Row) error {
			var sumAmount int32
			err := row.Scan(&sumAmount)
			assert.NoError(t, err)
			assert.EqualValues(t, 6, sumAmount)
			return nil
		})

		err := conn.SendBatch(context.Background(), batch).Close()
		assert.NoError(t, err)
	})
}

func TestConnSendBatchMany(t *testing.T) {
	t.Parallel()

	pgxtest.RunWithQueryExecModes(context.Background(), t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
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
	})
}

func TestConnSendBatchWithPreparedStatement(t *testing.T) {
	t.Parallel()

	modes := []pgx.QueryExecMode{
		pgx.QueryExecModeCacheStatement,
		pgx.QueryExecModeCacheDescribe,
		pgx.QueryExecModeDescribeExec,
		pgx.QueryExecModeExec,
		// Don't test simple mode with prepared statements.
	}
	pgxtest.RunWithQueryExecModes(context.Background(), t, defaultConnTestRunner, modes, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		pgxtest.SkipCockroachDB(t, conn, "Server issues incorrect ParameterDescription (https://github.com/cockroachdb/cockroach/issues/60907)")
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
	})
}

func TestConnSendBatchWithQueryRewriter(t *testing.T) {
	t.Parallel()

	pgxtest.RunWithQueryExecModes(context.Background(), t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		batch := &pgx.Batch{}
		batch.Queue("something to be replaced", &testQueryRewriter{sql: "select $1::int", args: []any{1}})
		batch.Queue("something else to be replaced", &testQueryRewriter{sql: "select $1::text", args: []any{"hello"}})
		batch.Queue("more to be replaced", &testQueryRewriter{sql: "select $1::int", args: []any{3}})

		br := conn.SendBatch(context.Background(), batch)

		var n int32
		err := br.QueryRow().Scan(&n)
		require.NoError(t, err)
		require.EqualValues(t, 1, n)

		var s string
		err = br.QueryRow().Scan(&s)
		require.NoError(t, err)
		require.Equal(t, "hello", s)

		err = br.QueryRow().Scan(&n)
		require.NoError(t, err)
		require.EqualValues(t, 3, n)

		err = br.Close()
		require.NoError(t, err)
	})
}

// https://github.com/jackc/pgx/issues/856
func TestConnSendBatchWithPreparedStatementAndStatementCacheDisabled(t *testing.T) {
	t.Parallel()

	config, err := pgx.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)

	config.DefaultQueryExecMode = pgx.QueryExecModeDescribeExec
	config.StatementCacheCapacity = 0
	config.DescriptionCacheCapacity = 0

	conn := mustConnect(t, config)
	defer closeConn(t, conn)

	pgxtest.SkipCockroachDB(t, conn, "Server issues incorrect ParameterDescription (https://github.com/cockroachdb/cockroach/issues/60907)")

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

	pgxtest.RunWithQueryExecModes(context.Background(), t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {

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

	})
}

func TestConnSendBatchQueryError(t *testing.T) {
	t.Parallel()

	pgxtest.RunWithQueryExecModes(context.Background(), t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {

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
			t.Errorf("br.Close() => %v, want error code %v", err, 22012)
		}

	})
}

func TestConnSendBatchQuerySyntaxError(t *testing.T) {
	t.Parallel()

	pgxtest.RunWithQueryExecModes(context.Background(), t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {

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

	})
}

func TestConnSendBatchQueryRowInsert(t *testing.T) {
	t.Parallel()

	pgxtest.RunWithQueryExecModes(context.Background(), t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {

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

	})
}

func TestConnSendBatchQueryPartialReadInsert(t *testing.T) {
	t.Parallel()

	pgxtest.RunWithQueryExecModes(context.Background(), t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {

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

	})
}

func TestTxSendBatch(t *testing.T) {
	t.Parallel()

	pgxtest.RunWithQueryExecModes(context.Background(), t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {

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

	})
}

func TestTxSendBatchRollback(t *testing.T) {
	t.Parallel()

	pgxtest.RunWithQueryExecModes(context.Background(), t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {

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

	})
}

func TestConnBeginBatchDeferredError(t *testing.T) {
	t.Parallel()

	pgxtest.RunWithQueryExecModes(context.Background(), t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {

		pgxtest.SkipCockroachDB(t, conn, "Server does not support deferred constraint (https://github.com/cockroachdb/cockroach/issues/31632)")

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

	})
}

func TestConnSendBatchNoStatementCache(t *testing.T) {
	config := mustParseConfig(t, os.Getenv("PGX_TEST_DATABASE"))
	config.DefaultQueryExecMode = pgx.QueryExecModeDescribeExec
	config.StatementCacheCapacity = 0
	config.DescriptionCacheCapacity = 0

	conn := mustConnect(t, config)
	defer closeConn(t, conn)

	testConnSendBatch(t, conn, 3)
}

func TestConnSendBatchPrepareStatementCache(t *testing.T) {
	config := mustParseConfig(t, os.Getenv("PGX_TEST_DATABASE"))
	config.DefaultQueryExecMode = pgx.QueryExecModeCacheStatement
	config.StatementCacheCapacity = 32

	conn := mustConnect(t, config)
	defer closeConn(t, conn)

	testConnSendBatch(t, conn, 3)
}

func TestConnSendBatchDescribeStatementCache(t *testing.T) {
	config := mustParseConfig(t, os.Getenv("PGX_TEST_DATABASE"))
	config.DefaultQueryExecMode = pgx.QueryExecModeCacheDescribe
	config.DescriptionCacheCapacity = 32

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

func TestSendBatchSimpleProtocol(t *testing.T) {
	t.Parallel()

	config := mustParseConfig(t, os.Getenv("PGX_TEST_DATABASE"))
	config.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

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
	assert.EqualValues(t, 1, values[0])
	assert.False(t, rows.Next())

	rows, err = results.Query()
	assert.NoError(t, err)
	assert.True(t, rows.Next())
	values, err = rows.Values()
	assert.NoError(t, err)
	assert.EqualValues(t, 2, values[0])
	assert.False(t, rows.Next())

	rows, err = results.Query()
	assert.NoError(t, err)
	assert.True(t, rows.Next())
	values, err = rows.Values()
	assert.NoError(t, err)
	assert.EqualValues(t, 3, values[0])
	assert.False(t, rows.Next())
}

func ExampleConn_SendBatch() {
	conn, err := pgx.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	if err != nil {
		fmt.Printf("Unable to establish connection: %v", err)
		return
	}

	batch := &pgx.Batch{}
	batch.Queue("select 1 + 1").QueryRow(func(row pgx.Row) error {
		var n int32
		err := row.Scan(&n)
		if err != nil {
			return err
		}

		fmt.Println(n)

		return err
	})

	batch.Queue("select 1 + 2").QueryRow(func(row pgx.Row) error {
		var n int32
		err := row.Scan(&n)
		if err != nil {
			return err
		}

		fmt.Println(n)

		return err
	})

	batch.Queue("select 2 + 3").QueryRow(func(row pgx.Row) error {
		var n int32
		err := row.Scan(&n)
		if err != nil {
			return err
		}

		fmt.Println(n)

		return err
	})

	err = conn.SendBatch(context.Background(), batch).Close()
	if err != nil {
		fmt.Printf("SendBatch error: %v", err)
		return
	}

	// Output:
	// 2
	// 3
	// 5
}
