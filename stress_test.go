package pgx_test

import (
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/jackc/fake"
	"github.com/jackc/pgx"
)

type execer interface {
	Exec(sql string, arguments ...interface{}) (commandTag pgx.CommandTag, err error)
}
type queryer interface {
	Query(sql string, args ...interface{}) (*pgx.Rows, error)
}
type queryRower interface {
	QueryRow(sql string, args ...interface{}) *pgx.Row
}

func TestStressConnPool(t *testing.T) {
	maxConnections := 8
	pool := createConnPool(t, maxConnections)
	defer pool.Close()

	setupStressDB(t, pool)

	actions := []struct {
		name string
		fn   func(*pgx.ConnPool, int) error
	}{
		{"insertUnprepared", func(p *pgx.ConnPool, n int) error { return insertUnprepared(p, n) }},
		{"queryRowWithoutParams", func(p *pgx.ConnPool, n int) error { return queryRowWithoutParams(p, n) }},
		{"query", func(p *pgx.ConnPool, n int) error { return queryCloseEarly(p, n) }},
		{"queryCloseEarly", func(p *pgx.ConnPool, n int) error { return query(p, n) }},
		{"queryErrorWhileReturningRows", func(p *pgx.ConnPool, n int) error { return queryErrorWhileReturningRows(p, n) }},
		{"txInsertRollback", txInsertRollback},
		{"txInsertCommit", txInsertCommit},
		{"txMultipleQueries", txMultipleQueries},
		{"notify", notify},
		{"listenAndPoolUnlistens", listenAndPoolUnlistens},
		{"reset", func(p *pgx.ConnPool, n int) error { p.Reset(); return nil }},
		{"poolPrepareUseAndDeallocate", poolPrepareUseAndDeallocate},
	}

	var timer *time.Timer
	if testing.Short() {
		timer = time.NewTimer(5 * time.Second)
	} else {
		timer = time.NewTimer(60 * time.Second)
	}
	workerCount := 16

	workChan := make(chan int)
	doneChan := make(chan struct{})
	errChan := make(chan error)

	work := func() {
		for n := range workChan {
			action := actions[rand.Intn(len(actions))]
			err := action.fn(pool, n)
			if err != nil {
				errChan <- err
				break
			}
		}
		doneChan <- struct{}{}
	}

	for i := 0; i < workerCount; i++ {
		go work()
	}

	var stop bool
	for i := 0; !stop; i++ {
		select {
		case <-timer.C:
			stop = true
		case workChan <- i:
		case err := <-errChan:
			close(workChan)
			t.Fatal(err)
		}
	}
	close(workChan)

	for i := 0; i < workerCount; i++ {
		<-doneChan
	}
}

func TestStressTLSConnection(t *testing.T) {
	t.Parallel()

	if tlsConnConfig == nil {
		t.Skip("Skipping due to undefined tlsConnConfig")
	}

	if testing.Short() {
		t.Skip("Skipping due to testing -short")
	}

	conn, err := pgx.Connect(*tlsConnConfig)
	if err != nil {
		t.Fatalf("Unable to establish connection: %v", err)
	}
	defer conn.Close()

	for i := 0; i < 50; i++ {
		sql := `select * from generate_series(1, $1)`

		rows, err := conn.Query(sql, 2000000)
		if err != nil {
			t.Fatal(err)
		}

		var n int32
		for rows.Next() {
			rows.Scan(&n)
		}

		if rows.Err() != nil {
			t.Fatalf("queryCount: %d, Row number: %d. %v", i, n, rows.Err())
		}
	}
}

func setupStressDB(t *testing.T, pool *pgx.ConnPool) {
	_, err := pool.Exec(`
		drop table if exists widgets;
		create table widgets(
			id serial primary key,
			name varchar not null,
			description text,
			creation_time timestamptz
		);
`)
	if err != nil {
		t.Fatal(err)
	}
}

func insertUnprepared(e execer, actionNum int) error {
	sql := `
		insert into widgets(name, description, creation_time)
		values($1, $2, $3)`

	_, err := e.Exec(sql, fake.ProductName(), fake.Sentences(), time.Now())
	return err
}

func queryRowWithoutParams(qr queryRower, actionNum int) error {
	var id int32
	var name, description string
	var creationTime time.Time

	sql := `select * from widgets order by random() limit 1`

	err := qr.QueryRow(sql).Scan(&id, &name, &description, &creationTime)
	if err == pgx.ErrNoRows {
		return nil
	}
	return err
}

func query(q queryer, actionNum int) error {
	sql := `select * from widgets order by random() limit $1`

	rows, err := q.Query(sql, 10)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var id int32
		var name, description string
		var creationTime time.Time
		rows.Scan(&id, &name, &description, &creationTime)
	}

	return rows.Err()
}

func queryCloseEarly(q queryer, actionNum int) error {
	sql := `select * from generate_series(1,$1)`

	rows, err := q.Query(sql, 100)
	if err != nil {
		return err
	}
	defer rows.Close()

	for i := 0; i < 10 && rows.Next(); i++ {
		var n int32
		rows.Scan(&n)
	}
	rows.Close()

	return rows.Err()
}

func queryErrorWhileReturningRows(q queryer, actionNum int) error {
	// This query should divide by 0 within the first number of rows
	sql := `select 42 / (random() * 20)::integer from generate_series(1,100000)`

	rows, err := q.Query(sql)
	if err != nil {
		return nil
	}
	defer rows.Close()

	for rows.Next() {
		var n int32
		rows.Scan(&n)
	}

	if _, ok := rows.Err().(pgx.PgError); ok {
		return nil
	}
	return rows.Err()
}

func notify(pool *pgx.ConnPool, actionNum int) error {
	_, err := pool.Exec("notify stress")
	return err
}

func listenAndPoolUnlistens(pool *pgx.ConnPool, actionNum int) error {
	conn, err := pool.Acquire()
	if err != nil {
		return err
	}
	defer pool.Release(conn)

	err = conn.Listen("stress")
	if err != nil {
		return err
	}

	_, err = conn.WaitForNotification(100 * time.Millisecond)
	if err == pgx.ErrNotificationTimeout {
		return nil
	}
	return err
}

func poolPrepareUseAndDeallocate(pool *pgx.ConnPool, actionNum int) error {
	psName := fmt.Sprintf("poolPreparedStatement%d", actionNum)

	_, err := pool.Prepare(psName, "select $1::text")
	if err != nil {
		return err
	}

	var s string
	err = pool.QueryRow(psName, "hello").Scan(&s)
	if err != nil {
		return err
	}

	if s != "hello" {
		return fmt.Errorf("Prepared statement did not return expected value: %v", s)
	}

	return pool.Deallocate(psName)
}

func txInsertRollback(pool *pgx.ConnPool, actionNum int) error {
	tx, err := pool.Begin()
	if err != nil {
		return err
	}

	sql := `
		insert into widgets(name, description, creation_time)
		values($1, $2, $3)`

	_, err = tx.Exec(sql, fake.ProductName(), fake.Sentences(), time.Now())
	if err != nil {
		return err
	}

	return tx.Rollback()
}

func txInsertCommit(pool *pgx.ConnPool, actionNum int) error {
	tx, err := pool.Begin()
	if err != nil {
		return err
	}

	sql := `
		insert into widgets(name, description, creation_time)
		values($1, $2, $3)`

	_, err = tx.Exec(sql, fake.ProductName(), fake.Sentences(), time.Now())
	if err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

func txMultipleQueries(pool *pgx.ConnPool, actionNum int) error {
	tx, err := pool.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	errExpectedTxDeath := errors.New("Expected tx death")

	actions := []struct {
		name string
		fn   func() error
	}{
		{"insertUnprepared", func() error { return insertUnprepared(tx, actionNum) }},
		{"queryRowWithoutParams", func() error { return queryRowWithoutParams(tx, actionNum) }},
		{"query", func() error { return query(tx, actionNum) }},
		{"queryCloseEarly", func() error { return queryCloseEarly(tx, actionNum) }},
		{"queryErrorWhileReturningRows", func() error {
			err := queryErrorWhileReturningRows(tx, actionNum)
			if err != nil {
				return err
			}
			return errExpectedTxDeath
		}},
	}

	for i := 0; i < 20; i++ {
		action := actions[rand.Intn(len(actions))]
		err := action.fn()
		if err == errExpectedTxDeath {
			return nil
		} else if err != nil {
			return err
		}
	}

	return tx.Commit()
}
