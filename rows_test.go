package pgx_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxtest"
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

type testErrRowScanner string

func (ers *testErrRowScanner) ScanRow(rows pgx.Rows) error {
	return errors.New(string(*ers))
}

// https://github.com/jackc/pgx/issues/1654
func TestRowScannerErrorIsFatalToRows(t *testing.T) {
	t.Parallel()

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		s := testErrRowScanner("foo")
		err := conn.QueryRow(ctx, "select 'Adam' as name, 72 as height").Scan(&s)
		require.EqualError(t, err, "foo")
	})
}

func TestForEachRow(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgxtest.RunWithQueryExecModes(ctx, t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		var actualResults []any

		rows, _ := conn.Query(
			context.Background(),
			"select n, n * 2 from generate_series(1, $1) n",
			3,
		)
		var a, b int
		ct, err := pgx.ForEachRow(rows, []any{&a, &b}, func() error {
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

func TestForEachRowScanError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgxtest.RunWithQueryExecModes(ctx, t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		var actualResults []any

		rows, _ := conn.Query(
			context.Background(),
			"select 'foo', 'bar' from generate_series(1, $1) n",
			3,
		)
		var a, b int
		ct, err := pgx.ForEachRow(rows, []any{&a, &b}, func() error {
			actualResults = append(actualResults, []any{a, b})
			return nil
		})
		require.EqualError(t, err, "can't scan into dest[0]: cannot scan text (OID 25) in text format into *int")
		require.Equal(t, pgconn.CommandTag{}, ct)
	})
}

func TestForEachRowAbort(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgxtest.RunWithQueryExecModes(ctx, t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(
			context.Background(),
			"select n, n * 2 from generate_series(1, $1) n",
			3,
		)
		var a, b int
		ct, err := pgx.ForEachRow(rows, []any{&a, &b}, func() error {
			return errors.New("abort")
		})
		require.EqualError(t, err, "abort")
		require.Equal(t, pgconn.CommandTag{}, ct)
	})
}

func ExampleForEachRow() {
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
	_, err = pgx.ForEachRow(rows, []any{&a, &b}, func() error {
		fmt.Printf("%v, %v\n", a, b)
		return nil
	})
	if err != nil {
		fmt.Printf("ForEachRow error: %v", err)
		return
	}

	// Output:
	// 1, 2
	// 2, 4
	// 3, 6
}

func TestCollectRows(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select n from generate_series(0, 99) n`)
		numbers, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (int32, error) {
			var n int32
			err := row.Scan(&n)
			return n, err
		})
		require.NoError(t, err)

		assert.Len(t, numbers, 100)
		for i := range numbers {
			assert.Equal(t, int32(i), numbers[i])
		}
	})
}

func TestCollectRowsEmpty(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select n from generate_series(1, 0) n`)
		numbers, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (int32, error) {
			var n int32
			err := row.Scan(&n)
			return n, err
		})
		require.NoError(t, err)
		require.NotNil(t, numbers)

		assert.Empty(t, numbers)
	})
}

// This example uses CollectRows with a manually written collector function. In most cases RowTo, RowToAddrOf,
// RowToStructByPos, RowToAddrOfStructByPos, or another generic function would be used.
func ExampleCollectRows() {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, os.Getenv("PGX_TEST_DATABASE"))
	if err != nil {
		fmt.Printf("Unable to establish connection: %v", err)
		return
	}

	rows, _ := conn.Query(ctx, `select n from generate_series(1, 5) n`)
	numbers, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (int32, error) {
		var n int32
		err := row.Scan(&n)
		return n, err
	})
	if err != nil {
		fmt.Printf("CollectRows error: %v", err)
		return
	}

	fmt.Println(numbers)

	// Output:
	// [1 2 3 4 5]
}

func TestCollectOneRow(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select 42`)
		n, err := pgx.CollectOneRow(rows, func(row pgx.CollectableRow) (int32, error) {
			var n int32
			err := row.Scan(&n)
			return n, err
		})
		assert.NoError(t, err)
		assert.Equal(t, int32(42), n)
	})
}

func TestCollectOneRowNotFound(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select 42 where false`)
		n, err := pgx.CollectOneRow(rows, func(row pgx.CollectableRow) (int32, error) {
			var n int32
			err := row.Scan(&n)
			return n, err
		})
		assert.ErrorIs(t, err, pgx.ErrNoRows)
		assert.Equal(t, int32(0), n)
	})
}

func TestCollectOneRowIgnoresExtraRows(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select n from generate_series(42, 99) n`)
		n, err := pgx.CollectOneRow(rows, func(row pgx.CollectableRow) (int32, error) {
			var n int32
			err := row.Scan(&n)
			return n, err
		})
		require.NoError(t, err)

		assert.NoError(t, err)
		assert.Equal(t, int32(42), n)
	})
}

// https://github.com/jackc/pgx/issues/1334
func TestCollectOneRowPrefersPostgreSQLErrorOverErrNoRows(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		_, err := conn.Exec(ctx, `create temporary table t (name text not null unique)`)
		require.NoError(t, err)

		var name string
		rows, _ := conn.Query(ctx, `insert into t (name) values ('foo') returning name`)
		name, err = pgx.CollectOneRow(rows, func(row pgx.CollectableRow) (string, error) {
			var n string
			err := row.Scan(&n)
			return n, err
		})
		require.NoError(t, err)
		require.Equal(t, "foo", name)

		rows, _ = conn.Query(ctx, `insert into t (name) values ('foo') returning name`)
		name, err = pgx.CollectOneRow(rows, func(row pgx.CollectableRow) (string, error) {
			var n string
			err := row.Scan(&n)
			return n, err
		})
		require.Error(t, err)
		var pgErr *pgconn.PgError
		require.ErrorAs(t, err, &pgErr)
		require.Equal(t, "23505", pgErr.Code)
		require.Equal(t, "", name)
	})
}

func TestCollectExactlyOneRow(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select 42`)
		n, err := pgx.CollectExactlyOneRow(rows, func(row pgx.CollectableRow) (int32, error) {
			var n int32
			err := row.Scan(&n)
			return n, err
		})
		assert.NoError(t, err)
		assert.Equal(t, int32(42), n)
	})
}

func TestCollectExactlyOneRowNotFound(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select 42 where false`)
		n, err := pgx.CollectExactlyOneRow(rows, func(row pgx.CollectableRow) (int32, error) {
			var n int32
			err := row.Scan(&n)
			return n, err
		})
		assert.ErrorIs(t, err, pgx.ErrNoRows)
		assert.Equal(t, int32(0), n)
	})
}

func TestCollectExactlyOneRowExtraRows(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select n from generate_series(42, 99) n`)
		n, err := pgx.CollectExactlyOneRow(rows, func(row pgx.CollectableRow) (int32, error) {
			var n int32
			err := row.Scan(&n)
			return n, err
		})
		assert.ErrorIs(t, err, pgx.ErrTooManyRows)
		assert.Equal(t, int32(0), n)
	})
}

func TestRowTo(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select n from generate_series(0, 99) n`)
		numbers, err := pgx.CollectRows(rows, pgx.RowTo[int32])
		require.NoError(t, err)

		assert.Len(t, numbers, 100)
		for i := range numbers {
			assert.Equal(t, int32(i), numbers[i])
		}
	})
}

func ExampleRowTo() {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, os.Getenv("PGX_TEST_DATABASE"))
	if err != nil {
		fmt.Printf("Unable to establish connection: %v", err)
		return
	}

	rows, _ := conn.Query(ctx, `select n from generate_series(1, 5) n`)
	numbers, err := pgx.CollectRows(rows, pgx.RowTo[int32])
	if err != nil {
		fmt.Printf("CollectRows error: %v", err)
		return
	}

	fmt.Println(numbers)

	// Output:
	// [1 2 3 4 5]
}

func TestRowToAddrOf(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select n from generate_series(0, 99) n`)
		numbers, err := pgx.CollectRows(rows, pgx.RowToAddrOf[int32])
		require.NoError(t, err)

		assert.Len(t, numbers, 100)
		for i := range numbers {
			assert.Equal(t, int32(i), *numbers[i])
		}
	})
}

func ExampleRowToAddrOf() {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, os.Getenv("PGX_TEST_DATABASE"))
	if err != nil {
		fmt.Printf("Unable to establish connection: %v", err)
		return
	}

	rows, _ := conn.Query(ctx, `select n from generate_series(1, 5) n`)
	pNumbers, err := pgx.CollectRows(rows, pgx.RowToAddrOf[int32])
	if err != nil {
		fmt.Printf("CollectRows error: %v", err)
		return
	}

	for _, p := range pNumbers {
		fmt.Println(*p)
	}

	// Output:
	// 1
	// 2
	// 3
	// 4
	// 5
}

func TestRowToMap(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select 'Joe' as name, n as age from generate_series(0, 9) n`)
		slice, err := pgx.CollectRows(rows, pgx.RowToMap)
		require.NoError(t, err)

		assert.Len(t, slice, 10)
		for i := range slice {
			assert.Equal(t, "Joe", slice[i]["name"])
			assert.EqualValues(t, i, slice[i]["age"])
		}
	})
}

func TestRowToStructByPos(t *testing.T) {
	type person struct {
		Name string
		Age  int32
	}

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select 'Joe' as name, n as age from generate_series(0, 9) n`)
		slice, err := pgx.CollectRows(rows, pgx.RowToStructByPos[person])
		require.NoError(t, err)

		assert.Len(t, slice, 10)
		for i := range slice {
			assert.Equal(t, "Joe", slice[i].Name)
			assert.EqualValues(t, i, slice[i].Age)
		}
	})
}

func TestRowToStructByPosIgnoredField(t *testing.T) {
	type person struct {
		Name string
		Age  int32 `db:"-"`
	}

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select 'Joe' as name from generate_series(0, 9) n`)
		slice, err := pgx.CollectRows(rows, pgx.RowToStructByPos[person])
		require.NoError(t, err)

		assert.Len(t, slice, 10)
		for i := range slice {
			assert.Equal(t, "Joe", slice[i].Name)
		}
	})
}

func TestRowToStructByPosEmbeddedStruct(t *testing.T) {
	type Name struct {
		First string
		Last  string
	}

	type person struct {
		Name
		Age int32
	}

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select 'John' as first_name, 'Smith' as last_name, n as age from generate_series(0, 9) n`)
		slice, err := pgx.CollectRows(rows, pgx.RowToStructByPos[person])
		require.NoError(t, err)

		assert.Len(t, slice, 10)
		for i := range slice {
			assert.Equal(t, "John", slice[i].Name.First)
			assert.Equal(t, "Smith", slice[i].Name.Last)
			assert.EqualValues(t, i, slice[i].Age)
		}
	})
}

func TestRowToStructByPosMultipleEmbeddedStruct(t *testing.T) {
	type Sandwich struct {
		Bread string
		Salad string
	}
	type Drink struct {
		Ml int
	}

	type meal struct {
		Sandwich
		Drink
	}

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select 'Baguette' as bread, 'Lettuce' as salad, drink_ml from generate_series(0, 9) drink_ml`)
		slice, err := pgx.CollectRows(rows, pgx.RowToStructByPos[meal])
		require.NoError(t, err)

		assert.Len(t, slice, 10)
		for i := range slice {
			assert.Equal(t, "Baguette", slice[i].Sandwich.Bread)
			assert.Equal(t, "Lettuce", slice[i].Sandwich.Salad)
			assert.EqualValues(t, i, slice[i].Drink.Ml)
		}
	})
}

func TestRowToStructByPosEmbeddedUnexportedStruct(t *testing.T) {
	type name struct {
		First string
		Last  string
	}

	type person struct {
		name
		Age int32
	}

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select 'John' as first_name, 'Smith' as last_name, n as age from generate_series(0, 9) n`)
		slice, err := pgx.CollectRows(rows, pgx.RowToStructByPos[person])
		require.NoError(t, err)

		assert.Len(t, slice, 10)
		for i := range slice {
			assert.Equal(t, "John", slice[i].name.First)
			assert.Equal(t, "Smith", slice[i].name.Last)
			assert.EqualValues(t, i, slice[i].Age)
		}
	})
}

// Pointer to struct is not supported. But check that we don't panic.
func TestRowToStructByPosEmbeddedPointerToStruct(t *testing.T) {
	type Name struct {
		First string
		Last  string
	}

	type person struct {
		*Name
		Age int32
	}

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select 'John' as first_name, 'Smith' as last_name, n as age from generate_series(0, 9) n`)
		_, err := pgx.CollectRows(rows, pgx.RowToStructByPos[person])
		require.EqualError(t, err, "got 3 values, but dst struct has only 2 fields")
	})
}

func ExampleRowToStructByPos() {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, os.Getenv("PGX_TEST_DATABASE"))
	if err != nil {
		fmt.Printf("Unable to establish connection: %v", err)
		return
	}

	if conn.PgConn().ParameterStatus("crdb_version") != "" {
		// Skip test / example when running on CockroachDB. Since an example can't be skipped fake success instead.
		fmt.Println(`Cheeseburger: $10
Fries: $5
Soft Drink: $3`)
		return
	}

	// Setup example schema and data.
	_, err = conn.Exec(ctx, `
create temporary table products (
	id int primary key generated by default as identity,
	name varchar(100) not null,
	price int not null
);

insert into products (name, price) values
	('Cheeseburger', 10),
	('Double Cheeseburger', 14),
	('Fries', 5),
	('Soft Drink', 3);
`)
	if err != nil {
		fmt.Printf("Unable to setup example schema and data: %v", err)
		return
	}

	type product struct {
		ID    int32
		Name  string
		Price int32
	}

	rows, _ := conn.Query(ctx, "select * from products where price < $1 order by price desc", 12)
	products, err := pgx.CollectRows(rows, pgx.RowToStructByPos[product])
	if err != nil {
		fmt.Printf("CollectRows error: %v", err)
		return
	}

	for _, p := range products {
		fmt.Printf("%s: $%d\n", p.Name, p.Price)
	}

	// Output:
	// Cheeseburger: $10
	// Fries: $5
	// Soft Drink: $3
}

func TestRowToAddrOfStructPos(t *testing.T) {
	type person struct {
		Name string
		Age  int32
	}

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select 'Joe' as name, n as age from generate_series(0, 9) n`)
		slice, err := pgx.CollectRows(rows, pgx.RowToAddrOfStructByPos[person])
		require.NoError(t, err)

		assert.Len(t, slice, 10)
		for i := range slice {
			assert.Equal(t, "Joe", slice[i].Name)
			assert.EqualValues(t, i, slice[i].Age)
		}
	})
}

func TestRowToStructByName(t *testing.T) {
	type person struct {
		Last      string
		First     string
		Age       int32
		AccountID string
	}

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select 'John' as first, 'Smith' as last, n as age, 'd5e49d3f' as account_id from generate_series(0, 9) n`)
		slice, err := pgx.CollectRows(rows, pgx.RowToStructByName[person])
		assert.NoError(t, err)

		assert.Len(t, slice, 10)
		for i := range slice {
			assert.Equal(t, "Smith", slice[i].Last)
			assert.Equal(t, "John", slice[i].First)
			assert.EqualValues(t, i, slice[i].Age)
			assert.Equal(t, "d5e49d3f", slice[i].AccountID)
		}

		// check missing fields in a returned row
		rows, _ = conn.Query(ctx, `select 'Smith' as last, n as age from generate_series(0, 9) n`)
		_, err = pgx.CollectRows(rows, pgx.RowToStructByName[person])
		assert.ErrorContains(t, err, "cannot find field First in returned row")

		// check missing field in a destination struct
		rows, _ = conn.Query(ctx, `select 'John' as first, 'Smith' as last, n as age, 'd5e49d3f' as account_id, null as ignore from generate_series(0, 9) n`)
		_, err = pgx.CollectRows(rows, pgx.RowToAddrOfStructByName[person])
		assert.ErrorContains(t, err, "struct doesn't have corresponding row field ignore")
	})
}

func TestRowToStructByNameDoublePointer(t *testing.T) {
	type person struct {
		Last      string
		First     **string
		Age       int32
		AccountID string
	}

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select 'John' as first, 'Smith' as last, n as age, 'd5e49d3f' as account_id from generate_series(0, 9) n`)
		slice, err := pgx.CollectRows(rows, pgx.RowToStructByName[person])
		assert.NoError(t, err)

		assert.Len(t, slice, 10)
		for i := range slice {
			assert.Equal(t, "Smith", slice[i].Last)
			assert.Equal(t, "John", **slice[i].First)
			assert.EqualValues(t, i, slice[i].Age)
			assert.Equal(t, "d5e49d3f", slice[i].AccountID)
		}

		rows, _ = conn.Query(ctx, `select NULL as first, 'Smith' as last, n as age, 'd5e49d3f' as account_id from generate_series(0, 9) n`)
		slice, err = pgx.CollectRows(rows, pgx.RowToStructByName[person])
		assert.NoError(t, err)

		assert.Len(t, slice, 10)
		for i := range slice {
			assert.Equal(t, "Smith", slice[i].Last)
			assert.Nil(t, slice[i].First)
			assert.EqualValues(t, i, slice[i].Age)
			assert.Equal(t, "d5e49d3f", slice[i].AccountID)
		}
	})
}

func TestRowToStructByNameTriplePointer(t *testing.T) {
	type person struct {
		Last      string
		First     ***string
		Age       int32
		AccountID string
	}

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select 'John' as first, 'Smith' as last, n as age, 'd5e49d3f' as account_id from generate_series(0, 9) n`)
		slice, err := pgx.CollectRows(rows, pgx.RowToStructByName[person])
		assert.NoError(t, err)

		assert.Len(t, slice, 10)
		for i := range slice {
			assert.Equal(t, "Smith", slice[i].Last)
			assert.Equal(t, "John", ***slice[i].First)
			assert.EqualValues(t, i, slice[i].Age)
			assert.Equal(t, "d5e49d3f", slice[i].AccountID)
		}

		rows, _ = conn.Query(ctx, `select NULL as first, 'Smith' as last, n as age, 'd5e49d3f' as account_id from generate_series(0, 9) n`)
		slice, err = pgx.CollectRows(rows, pgx.RowToStructByName[person])
		assert.NoError(t, err)

		assert.Len(t, slice, 10)
		for i := range slice {
			assert.Equal(t, "Smith", slice[i].Last)
			assert.Nil(t, slice[i].First)
			assert.EqualValues(t, i, slice[i].Age)
			assert.Equal(t, "d5e49d3f", slice[i].AccountID)
		}
	})
}

func TestRowToStructByNameDbTags(t *testing.T) {
	type person struct {
		Last             string `db:"last_name"`
		First            string `db:"first_name"`
		Age              int32  `db:"age"`
		AccountID        string `db:"account_id"`
		AnotherAccountID string `db:"account__id"`
	}

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select 'John' as first_name, 'Smith' as last_name, n as age, 'd5e49d3f' as account_id, '5e49d321' as account__id from generate_series(0, 9) n`)
		slice, err := pgx.CollectRows(rows, pgx.RowToStructByName[person])
		assert.NoError(t, err)

		assert.Len(t, slice, 10)
		for i := range slice {
			assert.Equal(t, "Smith", slice[i].Last)
			assert.Equal(t, "John", slice[i].First)
			assert.EqualValues(t, i, slice[i].Age)
			assert.Equal(t, "d5e49d3f", slice[i].AccountID)
			assert.Equal(t, "5e49d321", slice[i].AnotherAccountID)
		}

		// check missing fields in a returned row
		rows, _ = conn.Query(ctx, `select 'Smith' as last_name, n as age from generate_series(0, 9) n`)
		_, err = pgx.CollectRows(rows, pgx.RowToStructByName[person])
		assert.ErrorContains(t, err, "cannot find field first_name in returned row")

		// check missing field in a destination struct
		rows, _ = conn.Query(ctx, `select 'John' as first_name, 'Smith' as last_name, n as age, 'd5e49d3f' as account_id, '5e49d321' as account__id, null as ignore from generate_series(0, 9) n`)
		_, err = pgx.CollectRows(rows, pgx.RowToAddrOfStructByName[person])
		assert.ErrorContains(t, err, "struct doesn't have corresponding row field ignore")
	})
}

// A db tag must match its column case-insensitively. PostgreSQL folds unquoted
// identifiers to lower case, so a column aliased `as Region` comes back as
// "region" and has to match a `db:"Region"` tag. See
// https://github.com/jackc/pgx/issues/2296.
func TestRowToStructByNameDbTagsCaseInsensitive(t *testing.T) {
	type record struct {
		Region string `db:"Region"`
	}

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select 'east' as Region`)
		slice, err := pgx.CollectRows(rows, pgx.RowToStructByName[record])
		require.NoError(t, err)
		require.Len(t, slice, 1)
		assert.Equal(t, "east", slice[0].Region)
	})
}

func TestRowToStructByNameDbTagsPreferExactMatch(t *testing.T) {
	type record struct {
		Lower  int32  `db:"foo"`
		Upper  int32  `db:"FOO"`
		Region string `db:"Region"`
	}

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		for _, query := range []string{
			`select 1 as foo, 2 as "FOO", 'east' as region`,
			`select 2 as "FOO", 1 as foo, 'east' as region`,
		} {
			rows, err := conn.Query(ctx, query)
			require.NoError(t, err)
			got, err := pgx.CollectOneRow(rows, pgx.RowToStructByName[record])
			require.NoError(t, err)
			require.Equal(t, record{Lower: 1, Upper: 2, Region: "east"}, got)
		}
	})
}

func TestRowToStructByNameEmbeddedStruct(t *testing.T) {
	type Name struct {
		Last  string `db:"last_name"`
		First string `db:"first_name"`
	}

	type person struct {
		Ignore bool `db:"-"`
		Name
		Age int32
	}

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select 'John' as first_name, 'Smith' as last_name, n as age from generate_series(0, 9) n`)
		slice, err := pgx.CollectRows(rows, pgx.RowToStructByName[person])
		assert.NoError(t, err)

		assert.Len(t, slice, 10)
		for i := range slice {
			assert.Equal(t, "Smith", slice[i].Name.Last)
			assert.Equal(t, "John", slice[i].Name.First)
			assert.EqualValues(t, i, slice[i].Age)
		}

		// check missing fields in a returned row
		rows, _ = conn.Query(ctx, `select 'Smith' as last_name, n as age from generate_series(0, 9) n`)
		_, err = pgx.CollectRows(rows, pgx.RowToStructByName[person])
		assert.ErrorContains(t, err, "cannot find field first_name in returned row")

		// check missing field in a destination struct
		rows, _ = conn.Query(ctx, `select 'John' as first_name, 'Smith' as last_name, n as age, null as ignore from generate_series(0, 9) n`)
		_, err = pgx.CollectRows(rows, pgx.RowToAddrOfStructByName[person])
		assert.ErrorContains(t, err, "struct doesn't have corresponding row field ignore")
	})
}

func ExampleRowToStructByName() {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, os.Getenv("PGX_TEST_DATABASE"))
	if err != nil {
		fmt.Printf("Unable to establish connection: %v", err)
		return
	}

	if conn.PgConn().ParameterStatus("crdb_version") != "" {
		// Skip test / example when running on CockroachDB. Since an example can't be skipped fake success instead.
		fmt.Println(`Cheeseburger: $10
Fries: $5
Soft Drink: $3`)
		return
	}

	// Setup example schema and data.
	_, err = conn.Exec(ctx, `
create temporary table products (
	id int primary key generated by default as identity,
	name varchar(100) not null,
	price int not null
);

insert into products (name, price) values
	('Cheeseburger', 10),
	('Double Cheeseburger', 14),
	('Fries', 5),
	('Soft Drink', 3);
`)
	if err != nil {
		fmt.Printf("Unable to setup example schema and data: %v", err)
		return
	}

	type product struct {
		ID    int32
		Name  string
		Price int32
	}

	rows, _ := conn.Query(ctx, "select * from products where price < $1 order by price desc", 12)
	products, err := pgx.CollectRows(rows, pgx.RowToStructByName[product])
	if err != nil {
		fmt.Printf("CollectRows error: %v", err)
		return
	}

	for _, p := range products {
		fmt.Printf("%s: $%d\n", p.Name, p.Price)
	}

	// Output:
	// Cheeseburger: $10
	// Fries: $5
	// Soft Drink: $3
}

func TestRowToStructByNameLax(t *testing.T) {
	type person struct {
		Last   string
		First  string
		Age    int32
		Ignore bool `db:"-"`
	}

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select 'John' as first, 'Smith' as last, n as age from generate_series(0, 9) n`)
		slice, err := pgx.CollectRows(rows, pgx.RowToStructByNameLax[person])
		assert.NoError(t, err)

		assert.Len(t, slice, 10)
		for i := range slice {
			assert.Equal(t, "Smith", slice[i].Last)
			assert.Equal(t, "John", slice[i].First)
			assert.EqualValues(t, i, slice[i].Age)
		}

		// check missing fields in a returned row
		rows, _ = conn.Query(ctx, `select 'John' as first, n as age from generate_series(0, 9) n`)
		slice, err = pgx.CollectRows(rows, pgx.RowToStructByNameLax[person])
		assert.NoError(t, err)

		assert.Len(t, slice, 10)
		for i := range slice {
			assert.Equal(t, "John", slice[i].First)
			assert.EqualValues(t, i, slice[i].Age)
		}

		// check extra fields in a returned row
		rows, _ = conn.Query(ctx, `select 'John' as first, 'Smith' as last, n as age, null as ignore from generate_series(0, 9) n`)
		_, err = pgx.CollectRows(rows, pgx.RowToAddrOfStructByNameLax[person])
		assert.ErrorContains(t, err, "struct doesn't have corresponding row field ignore")

		// check missing fields in a destination struct
		rows, _ = conn.Query(ctx, `select 'Smith' as last, 'D.' as middle, n as age from generate_series(0, 9) n`)
		_, err = pgx.CollectRows(rows, pgx.RowToAddrOfStructByNameLax[person])
		assert.ErrorContains(t, err, "struct doesn't have corresponding row field middle")

		// check ignored fields in a destination struct
		rows, _ = conn.Query(ctx, `select 'Smith' as last, n as age, null as ignore from generate_series(0, 9) n`)
		_, err = pgx.CollectRows(rows, pgx.RowToAddrOfStructByNameLax[person])
		assert.ErrorContains(t, err, "struct doesn't have corresponding row field ignore")
	})
}

func TestRowToStructByNameLaxEmbeddedStruct(t *testing.T) {
	type Name struct {
		Last  string `db:"last_name"`
		First string `db:"first_name"`
	}

	type person struct {
		Ignore bool `db:"-"`
		Name
		Age int32
	}

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select 'John' as first_name, 'Smith' as last_name, n as age from generate_series(0, 9) n`)
		slice, err := pgx.CollectRows(rows, pgx.RowToStructByNameLax[person])
		assert.NoError(t, err)

		assert.Len(t, slice, 10)
		for i := range slice {
			assert.Equal(t, "Smith", slice[i].Name.Last)
			assert.Equal(t, "John", slice[i].Name.First)
			assert.EqualValues(t, i, slice[i].Age)
		}

		// check missing fields in a returned row
		rows, _ = conn.Query(ctx, `select 'John' as first_name, n as age from generate_series(0, 9) n`)
		slice, err = pgx.CollectRows(rows, pgx.RowToStructByNameLax[person])
		assert.NoError(t, err)

		assert.Len(t, slice, 10)
		for i := range slice {
			assert.Equal(t, "John", slice[i].Name.First)
			assert.EqualValues(t, i, slice[i].Age)
		}

		// check extra fields in a returned row
		rows, _ = conn.Query(ctx, `select 'John' as first_name, 'Smith' as last_name, n as age, null as ignore from generate_series(0, 9) n`)
		_, err = pgx.CollectRows(rows, pgx.RowToAddrOfStructByNameLax[person])
		assert.ErrorContains(t, err, "struct doesn't have corresponding row field ignore")

		// check missing fields in a destination struct
		rows, _ = conn.Query(ctx, `select 'Smith' as last_name, 'D.' as middle_name, n as age from generate_series(0, 9) n`)
		_, err = pgx.CollectRows(rows, pgx.RowToAddrOfStructByNameLax[person])
		assert.ErrorContains(t, err, "struct doesn't have corresponding row field middle_name")

		// check ignored fields in a destination struct
		rows, _ = conn.Query(ctx, `select 'Smith' as last_name, n as age, null as ignore from generate_series(0, 9) n`)
		_, err = pgx.CollectRows(rows, pgx.RowToAddrOfStructByNameLax[person])
		assert.ErrorContains(t, err, "struct doesn't have corresponding row field ignore")
	})
}

func TestRowToStructByNameLaxRowValue(t *testing.T) {
	type AnotherTable struct{}
	type User struct {
		UserID int    `json:"userId" db:"user_id"`
		Name   string `json:"name" db:"name"`
	}
	type UserAPIKey struct {
		UserAPIKeyID int `json:"userApiKeyId" db:"user_api_key_id"`
		UserID       int `json:"userId" db:"user_id"`

		User         *User         `json:"user" db:"user"`
		AnotherTable *AnotherTable `json:"anotherTable" db:"another_table"`
	}

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		pgxtest.SkipCockroachDB(t, conn, "")

		rows, _ := conn.Query(ctx, `
		WITH user_api_keys AS (
			SELECT 1 AS user_id, 101 AS user_api_key_id, 'abc123' AS api_key
		), users AS (
			SELECT 1 AS user_id, 'John Doe' AS name
		)
		SELECT user_api_keys.user_api_key_id, user_api_keys.user_id, row(users.*) AS user
		FROM user_api_keys
		LEFT JOIN users ON users.user_id = user_api_keys.user_id
		WHERE user_api_keys.api_key = 'abc123';
		`)
		slice, err := pgx.CollectRows(rows, pgx.RowToStructByNameLax[UserAPIKey])

		assert.NoError(t, err)
		assert.ElementsMatch(t, slice, []UserAPIKey{{UserAPIKeyID: 101, UserID: 1, User: &User{UserID: 1, Name: "John Doe"}, AnotherTable: nil}})
	})
}

func ExampleRowToStructByNameLax() {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, os.Getenv("PGX_TEST_DATABASE"))
	if err != nil {
		fmt.Printf("Unable to establish connection: %v", err)
		return
	}

	if conn.PgConn().ParameterStatus("crdb_version") != "" {
		// Skip test / example when running on CockroachDB. Since an example can't be skipped fake success instead.
		fmt.Println(`Cheeseburger: $10
Fries: $5
Soft Drink: $3`)
		return
	}

	// Setup example schema and data.
	_, err = conn.Exec(ctx, `
create temporary table products (
	id int primary key generated by default as identity,
	name varchar(100) not null,
	price int not null
);

insert into products (name, price) values
	('Cheeseburger', 10),
	('Double Cheeseburger', 14),
	('Fries', 5),
	('Soft Drink', 3);
`)
	if err != nil {
		fmt.Printf("Unable to setup example schema and data: %v", err)
		return
	}

	type product struct {
		ID    int32
		Name  string
		Type  string
		Price int32
	}

	rows, _ := conn.Query(ctx, "select * from products where price < $1 order by price desc", 12)
	products, err := pgx.CollectRows(rows, pgx.RowToStructByNameLax[product])
	if err != nil {
		fmt.Printf("CollectRows error: %v", err)
		return
	}

	for _, p := range products {
		fmt.Printf("%s: $%d\n", p.Name, p.Price)
	}

	// Output:
	// Cheeseburger: $10
	// Fries: $5
	// Soft Drink: $3
}

// Rows.TypeMap is available even when Rows.Conn is nil, so a caller can decode raw values itself.
func TestRowsTypeMapWithoutConn(t *testing.T) {
	t.Parallel()

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		pgxtest.SkipCockroachDB(t, conn, "Server does not support pipeline mode")

		pipeline := conn.PgConn().StartPipeline(ctx)
		pipeline.SendQueryParams(`select 'Adam'::text`, nil, nil, nil, nil)
		require.NoError(t, pipeline.Sync())

		results, err := pipeline.GetResults()
		require.NoError(t, err)
		rr, ok := results.(*pgconn.ResultReader)
		require.True(t, ok)

		rows := pgx.RowsFromResultReader(conn.TypeMap(), rr)
		require.Nil(t, rows.Conn(), "this Rows has no Conn, which is what makes TypeMap necessary")
		require.Same(t, conn.TypeMap(), rows.TypeMap())

		require.True(t, rows.Next())
		fd := rows.FieldDescriptions()[0]
		var name string
		require.NoError(t, rows.TypeMap().Scan(fd.DataTypeOID, fd.Format, rows.RawValues()[0], &name))
		require.Equal(t, "Adam", name)

		rows.Close()
		require.NoError(t, rows.Err())
		require.NoError(t, pipeline.Close())
	})
}

func TestRowPlan(t *testing.T) {
	t.Parallel()

	type item struct {
		ID   int32
		Name string
	}

	type customPlan struct {
		idIdx   int
		nameIdx int
		targets [2]any
	}

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		initCalls := 0
		customRowTo := func(row pgx.CollectableRow) (item, error) {
			plan, err := pgx.RowPlan(row, func(r pgx.CollectableRow) (*customPlan, error) {
				initCalls++
				p := &customPlan{idIdx: -1, nameIdx: -1}
				for i, fd := range r.FieldDescriptions() {
					switch fd.Name {
					case "id":
						p.idIdx = i
					case "name":
						p.nameIdx = i
					}
				}
				if p.idIdx == -1 || p.nameIdx == -1 {
					return nil, fmt.Errorf("missing required columns")
				}
				return p, nil
			})
			if err != nil {
				return item{}, err
			}
			var v item
			plan.targets[plan.idIdx] = &v.ID
			plan.targets[plan.nameIdx] = &v.Name
			err = row.Scan(plan.targets[:]...)
			return v, err
		}

		rows, err := conn.Query(ctx, `select n as id, 'name_' || n as name from generate_series(1, 50) n`)
		require.NoError(t, err)

		items, err := pgx.CollectRows(rows, customRowTo)
		require.NoError(t, err)
		require.Len(t, items, 50)
		require.Equal(t, 1, initCalls, "RowPlan init should be called once for the entire collection")
		require.Equal(t, item{ID: 1, Name: "name_1"}, items[0])
		require.Equal(t, item{ID: 50, Name: "name_50"}, items[49])

		// Works with CollectOneRow outside AppendRows as well.
		rowsOne, err := conn.Query(ctx, `select 99 as id, 'single' as name`)
		require.NoError(t, err)
		one, err := pgx.CollectOneRow(rowsOne, customRowTo)
		require.NoError(t, err)
		require.Equal(t, item{ID: 99, Name: "single"}, one)
		require.Equal(t, 2, initCalls)
	})
}

func TestRowPlanMultiplePlansAndError(t *testing.T) {
	t.Parallel()

	type planA struct{ col int }
	type planB struct{ col int }

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		initA, initB, failCalls := 0, 0, 0

		rows, err := conn.Query(ctx, `select n as a, n * 10 as b from generate_series(1, 10) n`)
		require.NoError(t, err)

		results, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) ([2]int32, error) {
			if failCalls == 0 {
				_, err := pgx.RowPlan(row, func(r pgx.CollectableRow) (*planA, error) {
					failCalls++
					return nil, fmt.Errorf("transient init error")
				})
				require.Error(t, err)
			}

			pa, err := pgx.RowPlan(row, func(r pgx.CollectableRow) (*planA, error) {
				initA++
				return &planA{col: 0}, nil
			})
			if err != nil {
				return [2]int32{}, err
			}
			pb, err := pgx.RowPlan(row, func(r pgx.CollectableRow) (*planB, error) {
				initB++
				return &planB{col: 1}, nil
			})
			if err != nil {
				return [2]int32{}, err
			}
			var out [2]int32
			err = row.Scan(&out[pa.col], &out[pb.col])
			return out, err
		})
		require.NoError(t, err)
		require.Len(t, results, 10)
		require.Equal(t, 1, initA)
		require.Equal(t, 1, initB)
		require.Equal(t, [2]int32{1, 10}, results[0])
		require.Equal(t, [2]int32{10, 100}, results[9])
	})
}

func TestRowToStructSliceAndPointerFieldIsolation(t *testing.T) {
	t.Parallel()

	type record struct {
		ID    int32    `db:"id"`
		Tags  []string `db:"tags"`
		Raw   []byte   `db:"raw"`
		Score *int32   `db:"score"`
	}

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		query := `
			select * from (values
				(1::int4, array['alpha', 'beta']::text[], '\x010203'::bytea, 100::int4),
				(2::int4, array['gamma']::text[],         '\x04'::bytea,     null::int4),
				(3::int4, null::text[],                   null::bytea,       300::int4)
			) as v(id, tags, raw, score)
		`

		rows, err := conn.Query(ctx, query)
		require.NoError(t, err)
		recs, err := pgx.CollectRows(rows, pgx.RowToStructByName[record])
		require.NoError(t, err)
		require.Len(t, recs, 3)

		require.Equal(t, []string{"alpha", "beta"}, recs[0].Tags)
		require.Equal(t, []byte{1, 2, 3}, recs[0].Raw)
		require.NotNil(t, recs[0].Score)
		require.EqualValues(t, 100, *recs[0].Score)

		require.Equal(t, []string{"gamma"}, recs[1].Tags)
		require.Equal(t, []byte{4}, recs[1].Raw)
		require.Nil(t, recs[1].Score)

		require.Nil(t, recs[2].Tags)
		require.Nil(t, recs[2].Raw)
		require.NotNil(t, recs[2].Score)
		require.EqualValues(t, 300, *recs[2].Score)
		require.NotSame(t, recs[0].Score, recs[2].Score)

		// Also verify AddrOf returns distinct struct pointers per row.
		rowsAddr, err := conn.Query(ctx, query)
		require.NoError(t, err)
		ptrs, err := pgx.CollectRows(rowsAddr, pgx.RowToAddrOfStructByName[record])
		require.NoError(t, err)
		require.Len(t, ptrs, 3)
		require.NotSame(t, ptrs[0], ptrs[1])
		require.NotSame(t, ptrs[1], ptrs[2])
		require.Equal(t, []string{"alpha", "beta"}, ptrs[0].Tags)
		require.Nil(t, ptrs[1].Score)
		require.EqualValues(t, 300, *ptrs[2].Score)
	})
}

func TestCollectRowsAlternatingScannersAndModes(t *testing.T) {
	t.Parallel()

	type PartA struct {
		A int32 `db:"a"`
		B int32 `db:"b"`
	}
	type PartB struct {
		B     int32 `db:"b"`
		A     int32 `db:"a"`
		Extra int32 `db:"extra"`
	}

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		// Switching from Lax (row 1) to Strict (row 2) when a field is missing must fail on row 2.
		rows, err := conn.Query(ctx, `select n as a, n * 2 as b from generate_series(1, 3) n`)
		require.NoError(t, err)

		rowNum := 0
		_, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (PartB, error) {
			rowNum++
			if rowNum == 1 {
				return pgx.RowToStructByNameLax[PartB](row)
			}
			return pgx.RowToStructByName[PartB](row)
		})
		require.ErrorContains(t, err, "cannot find field extra in returned row")

		// Alternating two different struct types and modes across rows succeeds.
		rows2, err := conn.Query(ctx, `select n as a, n * 2 as b from generate_series(1, 10) n`)
		require.NoError(t, err)

		rowNum = 0
		items, err := pgx.CollectRows(rows2, func(row pgx.CollectableRow) (PartA, error) {
			rowNum++
			if rowNum%2 == 0 {
				b, err := pgx.RowToStructByNameLax[PartB](row)
				return PartA{A: b.A, B: b.B}, err
			}
			if rowNum%3 == 0 {
				return pgx.RowToStructByPos[PartA](row)
			}
			return pgx.RowToStructByName[PartA](row)
		})
		require.NoError(t, err)
		require.Len(t, items, 10)
		for i, it := range items {
			n := int32(i + 1)
			require.Equal(t, PartA{A: n, B: n * 2}, it)
		}
	})
}

type nonComparableCustomRows struct {
	fds    []pgconn.FieldDescription
	data   [][2]int32
	idx    *int
	closed *bool
}

func (r nonComparableCustomRows) Close()                                       { *r.closed = true }
func (r nonComparableCustomRows) Err() error                                   { return nil }
func (r nonComparableCustomRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r nonComparableCustomRows) FieldDescriptions() []pgconn.FieldDescription { return r.fds }
func (r nonComparableCustomRows) Next() bool {
	*r.idx++
	if *r.idx >= len(r.data) {
		*r.closed = true
		return false
	}
	return true
}

func (r nonComparableCustomRows) Scan(dest ...any) error {
	row := r.data[*r.idx]
	for i := range dest {
		*dest[i].(*int32) = row[i]
	}
	return nil
}

func (r nonComparableCustomRows) Values() ([]any, error) {
	row := r.data[*r.idx]
	return []any{row[0], row[1]}, nil
}
func (r nonComparableCustomRows) RawValues() [][]byte  { return make([][]byte, len(r.fds)) }
func (r nonComparableCustomRows) Conn() *pgx.Conn      { return nil }
func (r nonComparableCustomRows) TypeMap() *pgtype.Map { return nil }

func TestCollectRowsNonComparableAndReusedCustomRows(t *testing.T) {
	t.Parallel()

	type Pair struct {
		A int32 `db:"a"`
		B int32 `db:"b"`
	}

	// 1. Value-type Rows containing slices is not comparable via interface `==`; must not panic.
	idx := -1
	closed := false
	r1 := nonComparableCustomRows{
		fds:    []pgconn.FieldDescription{{Name: "a"}, {Name: "b"}},
		data:   [][2]int32{{1, 2}, {3, 4}, {5, 6}},
		idx:    &idx,
		closed: &closed,
	}
	got1, err := pgx.CollectRows(r1, pgx.RowToStructByName[Pair])
	require.NoError(t, err)
	require.Equal(t, []Pair{{A: 1, B: 2}, {A: 3, B: 4}, {A: 5, B: 6}}, got1)

	// 2. Reusing the same pointer wrapper across a second query with swapped columns (b, a) must map accurately.
	idx = -1
	closed = false
	r2 := &nonComparableCustomRows{
		fds:    []pgconn.FieldDescription{{Name: "a"}, {Name: "b"}},
		data:   [][2]int32{{1, 2}, {3, 4}},
		idx:    &idx,
		closed: &closed,
	}
	got2a, err := pgx.CollectRows(r2, pgx.RowToStructByName[Pair])
	require.NoError(t, err)
	require.Equal(t, []Pair{{A: 1, B: 2}, {A: 3, B: 4}}, got2a)

	idx = -1
	closed = false
	r2.fds = []pgconn.FieldDescription{{Name: "b"}, {Name: "a"}}
	r2.data = [][2]int32{{20, 10}, {40, 30}}
	got2b, err := pgx.CollectRows(r2, pgx.RowToStructByName[Pair])
	require.NoError(t, err)
	require.Equal(t, []Pair{{A: 10, B: 20}, {A: 30, B: 40}}, got2b)
}
