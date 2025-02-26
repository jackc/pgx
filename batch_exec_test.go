package pgx_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxtest"
)

func TestBatchExec(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgxtest.RunWithQueryExecModes(ctx, t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		pgxtest.SkipCockroachDB(t, conn, "Server serial type is incompatible with test")

		sql := `CREATE TEMPORARY TABLE ledger(
	  id serial PRIMARY KEY,
	  description varchar NOT NULL,
	  amount int NOT NULL
	);`
		mustExec(t, conn, sql)

		var (
			batch *pgx.Batch
			err   error
		)

		err = pgx.BatchExec(ctx, conn, func(b *pgx.Batch) {
			b.Queue("INSERT INTO ledger(description, amount) VALUES($1, $2)", "q1", 1)
			b.Queue("INSERT INTO ledger(description, amount) VALUES($1, $2)", "q2", 2)
			b.Queue("INSERT INTO ledger(description, amount) VALUES($1, $2)", "q3", 3)

			batch = b // For checks
		})

		if err != nil {
			t.Error(err)
		}

		if batch.Len() != 3 {
			t.Errorf("batch.Len()size equal %v, want %v", batch.Len(), 3)
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

		rows, err := conn.Query(ctx, "SELECT id, description, amount FROM ledger ORDER BY id")
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
	})
}

func TestBatchExecTx(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgxtest.RunWithQueryExecModes(ctx, t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		pgxtest.SkipCockroachDB(t, conn, "Server serial type is incompatible with test")

		sql := `CREATE TEMPORARY TABLE ledger(
	  id serial PRIMARY KEY,
	  description varchar NOT NULL,
	  amount int NOT NULL
	);`
		mustExec(t, conn, sql)

		var (
			batch *pgx.Batch
			err   error
		)

		err = pgx.BatchExecTx(ctx, conn, func(b *pgx.Batch) {
			b.Queue("INSERT INTO ledger(description, amount) VALUES($1, $2)", "q1", 1)
			b.Queue("INSERT INTO ledger(description, amount) VALUES($1, $2)", "q2", 2)
			b.Queue("INSERT INTO ledger(description, amount) VALUES($1, $2)", "q3", 3)

			batch = b // For checks
		})

		if err != nil {
			t.Error(err)
		}

		if batch.Len() != 3 {
			t.Errorf("batch.Len()size equal %v, want %v", batch.Len(), 3)
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

		rows, err := conn.Query(ctx, "SELECT id, description, amount FROM ledger ORDER BY id")
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
	})
}
