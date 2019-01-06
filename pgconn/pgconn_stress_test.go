package pgconn_test

import (
	"context"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/pgconn"

	"github.com/stretchr/testify/require"
)

func TestConnStress(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.Nil(t, err)
	defer closeConn(t, pgConn)

	actionCount := 100
	if s := os.Getenv("PGX_TEST_STRESS_FACTOR"); s != "" {
		stressFactor, err := strconv.ParseInt(s, 10, 64)
		require.Nil(t, err, "Failed to parse PGX_TEST_STRESS_FACTOR")
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
			('a', 'b')`).ReadAll()
	require.Nil(t, err)
}

func stressExecSelect(pgConn *pgconn.PgConn) error {
	_, err := pgConn.Exec(context.Background(), "select * from widgets").ReadAll()
	return err
}

func stressExecParamsSelect(pgConn *pgconn.PgConn) error {
	result := pgConn.ExecParams(context.Background(), "select * from widgets where id < $1", [][]byte{[]byte("10")}, nil, nil, nil).Read()
	return result.Err
}

func stressBatch(pgConn *pgconn.PgConn) error {
	batch := &pgconn.Batch{}

	batch.ExecParams("select * from widgets", nil, nil, nil, nil)
	batch.ExecParams("select * from widgets where id < $1", [][]byte{[]byte("10")}, nil, nil, nil)
	_, err := pgConn.ExecBatch(context.Background(), batch).ReadAll()
	return err
}

func stressExecSelectCanceled(pgConn *pgconn.PgConn) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	_, err := pgConn.Exec(ctx, "select *, pg_sleep(1) from widgets").ReadAll()
	cancel()
	if err != context.DeadlineExceeded {
		return err
	}

	return nil
}

func stressExecParamsSelectCanceled(pgConn *pgconn.PgConn) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	result := pgConn.ExecParams(ctx, "select *, pg_sleep(1) from widgets where id < $1", [][]byte{[]byte("10")}, nil, nil, nil).Read()
	cancel()
	if result.Err != context.DeadlineExceeded {
		return result.Err
	}

	return nil
}

func stressBatchCanceled(pgConn *pgconn.PgConn) error {
	batch := &pgconn.Batch{}
	batch.ExecParams("select * from widgets", nil, nil, nil, nil)
	batch.ExecParams("select *, pg_sleep(1) from widgets where id < $1", [][]byte{[]byte("10")}, nil, nil, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	_, err := pgConn.ExecBatch(ctx, batch).ReadAll()
	cancel()
	if err != context.DeadlineExceeded {
		return err
	}

	return nil
}
