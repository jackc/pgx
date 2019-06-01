package pgconn_test

import (
	"context"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"testing"

	"github.com/jackc/pgconn"

	"github.com/stretchr/testify/require"
)

func TestConnStress(t *testing.T) {
	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_CONN_STRING"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	actionCount := 10000
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
	}

	for i := 0; i < actionCount; i++ {
		action := actions[rand.Intn(len(actions))]
		err := action.fn(pgConn)
		require.Nilf(t, err, "%d: %s", i, action.name)
	}

	// Each call with a context starts a goroutine. Ensure they are cleaned up when context is not canceled.
	numGoroutine := runtime.NumGoroutine()
	require.Truef(t, numGoroutine < 1000, "goroutines appear to be orphaned: %d in process", numGoroutine)
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
	require.NoError(t, err)
}

func stressExecSelect(pgConn *pgconn.PgConn) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := pgConn.Exec(ctx, "select * from widgets").ReadAll()
	return err
}

func stressExecParamsSelect(pgConn *pgconn.PgConn) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := pgConn.ExecParams(ctx, "select * from widgets where id < $1", [][]byte{[]byte("10")}, nil, nil, nil).Read()
	return result.Err
}

func stressBatch(pgConn *pgconn.PgConn) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batch := &pgconn.Batch{}

	batch.ExecParams("select * from widgets", nil, nil, nil, nil)
	batch.ExecParams("select * from widgets where id < $1", [][]byte{[]byte("10")}, nil, nil, nil)
	_, err := pgConn.ExecBatch(ctx, batch).ReadAll()
	return err
}
