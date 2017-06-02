package pgx_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"
)

func TestConnBeginBatch(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	sql := `create temporary table ledger(
  id serial primary key,
  description varchar not null,
  amount int not null
);`
	mustExec(t, conn, sql)

	batch := conn.BeginBatch()
	batch.Queue("insert into ledger(description, amount) values($1, $2)",
		[]interface{}{"q1", 1},
		[]pgtype.Oid{pgtype.VarcharOid, pgtype.Int4Oid},
		nil,
	)
	batch.Queue("insert into ledger(description, amount) values($1, $2)",
		[]interface{}{"q2", 2},
		[]pgtype.Oid{pgtype.VarcharOid, pgtype.Int4Oid},
		nil,
	)
	batch.Queue("insert into ledger(description, amount) values($1, $2)",
		[]interface{}{"q3", 3},
		[]pgtype.Oid{pgtype.VarcharOid, pgtype.Int4Oid},
		nil,
	)
	batch.Queue("select id, description, amount from ledger order by id",
		nil,
		nil,
		[]int16{pgx.BinaryFormatCode, pgx.TextFormatCode, pgx.BinaryFormatCode},
	)
	batch.Queue("select sum(amount) from ledger",
		nil,
		nil,
		[]int16{pgx.BinaryFormatCode},
	)

	err := batch.Send(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}

	ct, err := batch.ExecResults()
	if err != nil {
		t.Error(err)
	}
	if ct.RowsAffected() != 1 {
		t.Errorf("ct.RowsAffected() => %v, want %v", ct.RowsAffected(), 1)
	}

	ct, err = batch.ExecResults()
	if err != nil {
		t.Error(err)
	}
	if ct.RowsAffected() != 1 {
		t.Errorf("ct.RowsAffected() => %v, want %v", ct.RowsAffected(), 1)
	}

	rows, err := batch.QueryResults()
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

	err = batch.QueryRowResults().Scan(&amount)
	if err != nil {
		t.Error(err)
	}
	if amount != 6 {
		t.Errorf("amount => %v, want %v", amount, 6)
	}

	err = batch.Finish()
	if err != nil {
		t.Fatal(err)
	}

	ensureConnValid(t, conn)
}

func TestConnBeginBatchContextCancel(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	sql := `create temporary table ledger(
  id serial primary key,
  description varchar not null,
  amount int not null
);`
	mustExec(t, conn, sql)

	batch := conn.BeginBatch()
	batch.Queue("insert into ledger(description, amount) values($1, $2)",
		[]interface{}{"q1", 1},
		[]pgtype.Oid{pgtype.VarcharOid, pgtype.Int4Oid},
		nil,
	)
	batch.Queue("insert into ledger(description, amount) values($1, $2)",
		[]interface{}{"q2", 2},
		[]pgtype.Oid{pgtype.VarcharOid, pgtype.Int4Oid},
		nil,
	)
	batch.Queue("insert into ledger(description, amount) values($1, $2)",
		[]interface{}{"q3", 3},
		[]pgtype.Oid{pgtype.VarcharOid, pgtype.Int4Oid},
		nil,
	)
	batch.Queue("select id, description, amount from ledger order by id",
		nil,
		nil,
		[]int16{pgx.BinaryFormatCode, pgx.TextFormatCode, pgx.BinaryFormatCode},
	)
	batch.Queue("select sum(amount) from ledger",
		nil,
		nil,
		[]int16{pgx.BinaryFormatCode},
	)
	batch.Queue("select pg_sleep(2)",
		nil,
		nil,
		nil,
	)

	err := batch.Send(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}

	ct, err := batch.ExecResults()
	if err != nil {
		t.Error(err)
	}
	if ct.RowsAffected() != 1 {
		t.Errorf("ct.RowsAffected() => %v, want %v", ct.RowsAffected(), 1)
	}

	ct, err = batch.ExecResults()
	if err != nil {
		t.Error(err)
	}
	if ct.RowsAffected() != 1 {
		t.Errorf("ct.RowsAffected() => %v, want %v", ct.RowsAffected(), 1)
	}

	rows, err := batch.QueryResults()
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

	err = batch.QueryRowResults().Scan(&amount)
	if err != nil {
		t.Error(err)
	}
	if amount != 6 {
		t.Errorf("amount => %v, want %v", amount, 6)
	}

	err = batch.Finish()
	if err != nil {
		t.Fatal(err)
	}

	ensureConnValid(t, conn)
}
