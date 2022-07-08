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
	"github.com/stretchr/testify/require"
)

type testRowScanner struct {
	name string
	age  int32
}

func (rs *testRowScanner) ScanRow(rows pgx.Rows) error {
	return rows.Scan(&rs.name, &rs.age)
}

func TestRowScanner(t *testing.T) {
	t.Parallel()

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		var s testRowScanner
		err := conn.QueryRow(ctx, "select 'Adam' as name, 72 as height").Scan(&s)
		require.NoError(t, err)
		require.Equal(t, "Adam", s.name)
		require.Equal(t, int32(72), s.age)
	})
}

func TestForEachScannedRow(t *testing.T) {
	t.Parallel()

	pgxtest.RunWithQueryExecModes(context.Background(), t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		var actualResults []any

		rows, _ := conn.Query(
			context.Background(),
			"select n, n * 2 from generate_series(1, $1) n",
			3,
		)
		var a, b int
		ct, err := pgx.ForEachScannedRow(rows, []any{&a, &b}, func() error {
			actualResults = append(actualResults, []any{a, b})
			return nil
		})
		require.NoError(t, err)

		expectedResults := []any{
			[]any{1, 2},
			[]any{2, 4},
			[]any{3, 6},
		}
		require.Equal(t, expectedResults, actualResults)
		require.EqualValues(t, 3, ct.RowsAffected())
	})
}

func TestForEachScannedRowScanError(t *testing.T) {
	t.Parallel()

	pgxtest.RunWithQueryExecModes(context.Background(), t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		var actualResults []any

		rows, _ := conn.Query(
			context.Background(),
			"select 'foo', 'bar' from generate_series(1, $1) n",
			3,
		)
		var a, b int
		ct, err := pgx.ForEachScannedRow(rows, []any{&a, &b}, func() error {
			actualResults = append(actualResults, []any{a, b})
			return nil
		})
		require.EqualError(t, err, "can't scan into dest[0]: cannot scan OID 25 in text format into *int")
		require.Equal(t, pgconn.CommandTag{}, ct)
	})
}

func TestForEachScannedRowAbort(t *testing.T) {
	t.Parallel()

	pgxtest.RunWithQueryExecModes(context.Background(), t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(
			context.Background(),
			"select n, n * 2 from generate_series(1, $1) n",
			3,
		)
		var a, b int
		ct, err := pgx.ForEachScannedRow(rows, []any{&a, &b}, func() error {
			return errors.New("abort")
		})
		require.EqualError(t, err, "abort")
		require.Equal(t, pgconn.CommandTag{}, ct)
	})
}

func ExampleForEachScannedRow() {
	conn, err := pgx.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	if err != nil {
		fmt.Printf("Unable to establish connection: %v", err)
		return
	}

	rows, _ := conn.Query(
		context.Background(),
		"select n, n * 2 from generate_series(1, $1) n",
		3,
	)
	var a, b int
	_, err = pgx.ForEachScannedRow(rows, []any{&a, &b}, func() error {
		fmt.Printf("%v, %v\n", a, b)
		return nil
	})
	if err != nil {
		fmt.Printf("ForEachScannedRow error: %v", err)
		return
	}

	// Output:
	// 1, 2
	// 2, 4
	// 3, 6
}
