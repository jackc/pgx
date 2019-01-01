package pgconn_test

import (
	"context"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/pgconn"
	"github.com/pkg/errors"

	"github.com/stretchr/testify/require"
)

func TestConnStress(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.Nil(t, err)
	defer closeConn(t, pgConn)

	actionCount := 100
	if s := os.Getenv("PTX_TEST_STRESS_FACTOR"); s != "" {
		stressFactor, err := strconv.ParseInt(s, 10, 64)
		require.Nil(t, err, "Failed to parse PTX_TEST_STRESS_FACTOR")
		actionCount *= int(stressFactor)
	}

	setupStressDB(t, pgConn)

	actions := []struct {
		name string
		fn   func(*pgconn.PgConn) error
	}{
		{"Exec Select", stressExecSelect},
		{"ExecParams Select", stressExecParamsSelect},
		{"Batch", stressBatch},
		{"ExecCanceled", stressExecSelectCanceled},
		{"ExecParamsCanceled", stressExecParamsSelectCanceled},
		{"BatchCanceled", stressBatchCanceled},
	}

	for i := 0; i < actionCount; i++ {
		action := actions[rand.Intn(len(actions))]
		err := action.fn(pgConn)
		require.Nilf(t, err, "%d: %s", i, action.name)
	}
}

func setupStressDB(t *testing.T, pgConn *pgconn.PgConn) {
	_, err := pgConn.Exec(context.Background(), `
		create temporary table widgets(
			id serial primary key,
			name varchar not null,
			description text,
			creation_time timestamptz default now()
		);

		insert into widgets(name, description) values
			('Foo', 'bar'),
			('baz', 'Something really long Something really long Something really long Something really long Something really long'),
			('a', 'b')`)
	require.Nil(t, err)
}

func stressExecSelect(pgConn *pgconn.PgConn) error {
	_, err := pgConn.Exec(context.Background(), "select * from widgets")
	return err
}

func stressExecParamsSelect(pgConn *pgconn.PgConn) error {
	_, err := pgConn.ExecParams(context.Background(), "select * from widgets where id < $1", [][]byte{[]byte("10")}, nil, nil, nil)
	return err
}

func stressBatch(pgConn *pgconn.PgConn) error {
	pgConn.SendExec("select * from widgets")
	pgConn.SendExecParams("select * from widgets where id < $1", [][]byte{[]byte("10")}, nil, nil, nil)
	err := pgConn.Flush(context.Background())
	if err != nil {
		return err
	}

	// Query 1
	resultReader := pgConn.GetResult(context.Background())
	if resultReader == nil {
		return errors.New("missing resultReader")
	}

	for resultReader.NextRow() {
	}
	_, err = resultReader.Close()
	if err != nil {
		return err
	}

	// Query 2
	resultReader = pgConn.GetResult(context.Background())
	if resultReader == nil {
		return errors.New("missing resultReader")
	}

	for resultReader.NextRow() {
	}
	_, err = resultReader.Close()
	if err != nil {
		return err
	}

	// No more
	resultReader = pgConn.GetResult(context.Background())
	if resultReader != nil {
		return errors.New("unexpected result reader")
	}

	return nil
}

func stressExecSelectCanceled(pgConn *pgconn.PgConn) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	_, err := pgConn.Exec(ctx, "select *, pg_sleep(1) from widgets")
	cancel()
	if err != context.DeadlineExceeded {
		return err
	}

	ctx, cancel = context.WithTimeout(context.Background(), 500*time.Millisecond)
	recovered := pgConn.RecoverFromTimeout(ctx)
	cancel()
	if !recovered {
		return errors.New("unable to recover from timeout")
	}
	return nil
}

func stressExecParamsSelectCanceled(pgConn *pgconn.PgConn) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	_, err := pgConn.ExecParams(ctx, "select *, pg_sleep(1) from widgets where id < $1", [][]byte{[]byte("10")}, nil, nil, nil)
	cancel()
	if err != context.DeadlineExceeded {
		return err
	}

	ctx, cancel = context.WithTimeout(context.Background(), 500*time.Millisecond)
	recovered := pgConn.RecoverFromTimeout(ctx)
	cancel()
	if !recovered {
		return errors.New("unable to recover from timeout")
	}
	return nil
}

func stressBatchCanceled(pgConn *pgconn.PgConn) error {

	pgConn.SendExec("select * from widgets")
	pgConn.SendExecParams("select *, pg_sleep(1) from widgets where id < $1", [][]byte{[]byte("10")}, nil, nil, nil)
	err := pgConn.Flush(context.Background())
	if err != nil {
		return err
	}

	// Query 1
	resultReader := pgConn.GetResult(context.Background())
	if resultReader == nil {
		return errors.New("missing resultReader")
	}

	for resultReader.NextRow() {
	}
	_, err = resultReader.Close()
	if err != nil {
		return err
	}

	// Query 2
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	resultReader = pgConn.GetResult(ctx)
	cancel()
	if resultReader == nil {
		return errors.New("missing resultReader")
	}

	for resultReader.NextRow() {
	}
	_, err = resultReader.Close()
	if err != context.DeadlineExceeded {
		return err
	}

	ctx, cancel = context.WithTimeout(context.Background(), 500*time.Millisecond)
	recovered := pgConn.RecoverFromTimeout(ctx)
	cancel()
	if !recovered {
		return errors.New("unable to recover from timeout")
	}
	return nil
}
