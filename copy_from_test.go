package pgx_test

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxtest"
	"github.com/stretchr/testify/require"
)

func TestConnCopyWithAllQueryExecModes(t *testing.T) {
	for _, mode := range pgxtest.AllQueryExecModes {
		t.Run(mode.String(), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			defer cancel()

			cfg := mustParseConfig(t, os.Getenv("PGX_TEST_DATABASE"))
			cfg.DefaultQueryExecMode = mode
			conn := mustConnect(t, cfg)
			defer closeConn(t, conn)

			mustExec(t, conn, `create temporary table foo(
			a int2,
			b int4,
			c int8,
			d text,
			e timestamptz
		)`)

			tzedTime := time.Date(2010, 2, 3, 4, 5, 6, 0, time.Local)

			inputRows := [][]any{
				{int16(0), int32(1), int64(2), "abc", tzedTime},
				{nil, nil, nil, nil, nil},
			}

			copyCount, err := conn.CopyFrom(ctx, pgx.Identifier{"foo"}, []string{"a", "b", "c", "d", "e"}, pgx.CopyFromRows(inputRows))
			if err != nil {
				t.Errorf("Unexpected error for CopyFrom: %v", err)
			}
			if int(copyCount) != len(inputRows) {
				t.Errorf("Expected CopyFrom to return %d copied rows, but got %d", len(inputRows), copyCount)
			}

			rows, err := conn.Query(ctx, "select * from foo")
			if err != nil {
				t.Errorf("Unexpected error for Query: %v", err)
			}

			var outputRows [][]any
			for rows.Next() {
				row, err := rows.Values()
				if err != nil {
					t.Errorf("Unexpected error for rows.Values(): %v", err)
				}
				outputRows = append(outputRows, row)
			}

			if rows.Err() != nil {
				t.Errorf("Unexpected error for rows.Err(): %v", rows.Err())
			}

			if !reflect.DeepEqual(inputRows, outputRows) {
				t.Errorf("Input rows and output rows do not equal: %v -> %v", inputRows, outputRows)
			}

			ensureConnValid(t, conn)
		})
	}
}

func TestConnCopyWithKnownOIDQueryExecModes(t *testing.T) {
	for _, mode := range pgxtest.KnownOIDQueryExecModes {
		t.Run(mode.String(), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			defer cancel()

			cfg := mustParseConfig(t, os.Getenv("PGX_TEST_DATABASE"))
			cfg.DefaultQueryExecMode = mode
			conn := mustConnect(t, cfg)
			defer closeConn(t, conn)

			mustExec(t, conn, `create temporary table foo(
			a int2,
			b int4,
			c int8,
			d varchar,
			e text,
			f date,
			g timestamptz
		)`)

			tzedTime := time.Date(2010, 2, 3, 4, 5, 6, 0, time.Local)

			inputRows := [][]any{
				{int16(0), int32(1), int64(2), "abc", "efg", time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), tzedTime},
				{nil, nil, nil, nil, nil, nil, nil},
			}

			copyCount, err := conn.CopyFrom(ctx, pgx.Identifier{"foo"}, []string{"a", "b", "c", "d", "e", "f", "g"}, pgx.CopyFromRows(inputRows))
			if err != nil {
				t.Errorf("Unexpected error for CopyFrom: %v", err)
			}
			if int(copyCount) != len(inputRows) {
				t.Errorf("Expected CopyFrom to return %d copied rows, but got %d", len(inputRows), copyCount)
			}

			rows, err := conn.Query(ctx, "select * from foo")
			if err != nil {
				t.Errorf("Unexpected error for Query: %v", err)
			}

			var outputRows [][]any
			for rows.Next() {
				row, err := rows.Values()
				if err != nil {
					t.Errorf("Unexpected error for rows.Values(): %v", err)
				}
				outputRows = append(outputRows, row)
			}

			if rows.Err() != nil {
				t.Errorf("Unexpected error for rows.Err(): %v", rows.Err())
			}

			if !reflect.DeepEqual(inputRows, outputRows) {
				t.Errorf("Input rows and output rows do not equal: %v -> %v", inputRows, outputRows)
			}

			ensureConnValid(t, conn)
		})
	}
}

func TestConnCopyFromSmall(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	mustExec(t, conn, `create temporary table foo(
		a int2,
		b int4,
		c int8,
		d varchar,
		e text,
		f date,
		g timestamptz
	)`)

	tzedTime := time.Date(2010, 2, 3, 4, 5, 6, 0, time.Local)

	inputRows := [][]any{
		{int16(0), int32(1), int64(2), "abc", "efg", time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), tzedTime},
		{nil, nil, nil, nil, nil, nil, nil},
	}

	copyCount, err := conn.CopyFrom(ctx, pgx.Identifier{"foo"}, []string{"a", "b", "c", "d", "e", "f", "g"}, pgx.CopyFromRows(inputRows))
	if err != nil {
		t.Errorf("Unexpected error for CopyFrom: %v", err)
	}
	if int(copyCount) != len(inputRows) {
		t.Errorf("Expected CopyFrom to return %d copied rows, but got %d", len(inputRows), copyCount)
	}

	rows, err := conn.Query(ctx, "select * from foo")
	if err != nil {
		t.Errorf("Unexpected error for Query: %v", err)
	}

	var outputRows [][]any
	for rows.Next() {
		row, err := rows.Values()
		if err != nil {
			t.Errorf("Unexpected error for rows.Values(): %v", err)
		}
		outputRows = append(outputRows, row)
	}

	if rows.Err() != nil {
		t.Errorf("Unexpected error for rows.Err(): %v", rows.Err())
	}

	if !reflect.DeepEqual(inputRows, outputRows) {
		t.Errorf("Input rows and output rows do not equal: %v -> %v", inputRows, outputRows)
	}

	ensureConnValid(t, conn)
}

func TestConnCopyFromSliceSmall(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	mustExec(t, conn, `create temporary table foo(
		a int2,
		b int4,
		c int8,
		d varchar,
		e text,
		f date,
		g timestamptz
	)`)

	tzedTime := time.Date(2010, 2, 3, 4, 5, 6, 0, time.Local)

	inputRows := [][]any{
		{int16(0), int32(1), int64(2), "abc", "efg", time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), tzedTime},
		{nil, nil, nil, nil, nil, nil, nil},
	}

	copyCount, err := conn.CopyFrom(ctx, pgx.Identifier{"foo"}, []string{"a", "b", "c", "d", "e", "f", "g"},
		pgx.CopyFromSlice(len(inputRows), func(i int) ([]any, error) {
			return inputRows[i], nil
		}))
	if err != nil {
		t.Errorf("Unexpected error for CopyFrom: %v", err)
	}
	if int(copyCount) != len(inputRows) {
		t.Errorf("Expected CopyFrom to return %d copied rows, but got %d", len(inputRows), copyCount)
	}

	rows, err := conn.Query(ctx, "select * from foo")
	if err != nil {
		t.Errorf("Unexpected error for Query: %v", err)
	}

	var outputRows [][]any
	for rows.Next() {
		row, err := rows.Values()
		if err != nil {
			t.Errorf("Unexpected error for rows.Values(): %v", err)
		}
		outputRows = append(outputRows, row)
	}

	if rows.Err() != nil {
		t.Errorf("Unexpected error for rows.Err(): %v", rows.Err())
	}

	if !reflect.DeepEqual(inputRows, outputRows) {
		t.Errorf("Input rows and output rows do not equal: %v -> %v", inputRows, outputRows)
	}

	ensureConnValid(t, conn)
}

func TestConnCopyFromLarge(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	mustExec(t, conn, `create temporary table foo(
		a int2,
		b int4,
		c int8,
		d varchar,
		e text,
		f date,
		g timestamptz,
		h bytea
	)`)

	tzedTime := time.Date(2010, 2, 3, 4, 5, 6, 0, time.Local)

	inputRows := [][]any{}

	for range 10_000 {
		inputRows = append(inputRows, []any{int16(0), int32(1), int64(2), "abc", "efg", time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), tzedTime, []byte{111, 111, 111, 111}})
	}

	copyCount, err := conn.CopyFrom(ctx, pgx.Identifier{"foo"}, []string{"a", "b", "c", "d", "e", "f", "g", "h"}, pgx.CopyFromRows(inputRows))
	if err != nil {
		t.Errorf("Unexpected error for CopyFrom: %v", err)
	}
	if int(copyCount) != len(inputRows) {
		t.Errorf("Expected CopyFrom to return %d copied rows, but got %d", len(inputRows), copyCount)
	}

	rows, err := conn.Query(ctx, "select * from foo")
	if err != nil {
		t.Errorf("Unexpected error for Query: %v", err)
	}

	var outputRows [][]any
	for rows.Next() {
		row, err := rows.Values()
		if err != nil {
			t.Errorf("Unexpected error for rows.Values(): %v", err)
		}
		outputRows = append(outputRows, row)
	}

	if rows.Err() != nil {
		t.Errorf("Unexpected error for rows.Err(): %v", rows.Err())
	}

	if !reflect.DeepEqual(inputRows, outputRows) {
		t.Errorf("Input rows and output rows do not equal")
	}

	ensureConnValid(t, conn)
}

func TestConnCopyFromEnum(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	tx, err := conn.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `drop type if exists color`)
	require.NoError(t, err)

	_, err = tx.Exec(ctx, `drop type if exists fruit`)
	require.NoError(t, err)

	_, err = tx.Exec(ctx, `create type color as enum ('blue', 'green', 'orange')`)
	require.NoError(t, err)

	_, err = tx.Exec(ctx, `create type fruit as enum ('apple', 'orange', 'grape')`)
	require.NoError(t, err)

	// Obviously using conn while a tx is in use and registering a type after the connection has been established are
	// really bad practices, but for the sake of convenience we do it in the test here.
	for _, name := range []string{"fruit", "color"} {
		typ, err := conn.LoadType(ctx, name)
		require.NoError(t, err)
		conn.TypeMap().RegisterType(typ)
	}

	_, err = tx.Exec(ctx, `create temporary table foo(
		a text,
		b color,
		c fruit,
		d color,
		e fruit,
		f text
	)`)
	require.NoError(t, err)

	inputRows := [][]any{
		{"abc", "blue", "grape", "orange", "orange", "def"},
		{nil, nil, nil, nil, nil, nil},
	}

	copyCount, err := tx.CopyFrom(ctx, pgx.Identifier{"foo"}, []string{"a", "b", "c", "d", "e", "f"}, pgx.CopyFromRows(inputRows))
	require.NoError(t, err)
	require.EqualValues(t, len(inputRows), copyCount)

	rows, err := tx.Query(ctx, "select * from foo")
	require.NoError(t, err)

	var outputRows [][]any
	for rows.Next() {
		row, err := rows.Values()
		require.NoError(t, err)
		outputRows = append(outputRows, row)
	}

	require.NoError(t, rows.Err())

	if !reflect.DeepEqual(inputRows, outputRows) {
		t.Errorf("Input rows and output rows do not equal: %v -> %v", inputRows, outputRows)
	}

	err = tx.Rollback(ctx)
	require.NoError(t, err)

	ensureConnValid(t, conn)
}

func TestConnCopyFromJSON(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	for _, typeName := range []string{"json", "jsonb"} {
		if _, ok := conn.TypeMap().TypeForName(typeName); !ok {
			return // No JSON/JSONB type -- must be running against old PostgreSQL
		}
	}

	mustExec(t, conn, `create temporary table foo(
		a json,
		b jsonb
	)`)

	inputRows := [][]any{
		{map[string]any{"foo": "bar"}, map[string]any{"bar": "quz"}},
		{nil, nil},
	}

	copyCount, err := conn.CopyFrom(ctx, pgx.Identifier{"foo"}, []string{"a", "b"}, pgx.CopyFromRows(inputRows))
	if err != nil {
		t.Errorf("Unexpected error for CopyFrom: %v", err)
	}
	if int(copyCount) != len(inputRows) {
		t.Errorf("Expected CopyFrom to return %d copied rows, but got %d", len(inputRows), copyCount)
	}

	rows, err := conn.Query(ctx, "select * from foo")
	if err != nil {
		t.Errorf("Unexpected error for Query: %v", err)
	}

	var outputRows [][]any
	for rows.Next() {
		row, err := rows.Values()
		if err != nil {
			t.Errorf("Unexpected error for rows.Values(): %v", err)
		}
		outputRows = append(outputRows, row)
	}

	if rows.Err() != nil {
		t.Errorf("Unexpected error for rows.Err(): %v", rows.Err())
	}

	if !reflect.DeepEqual(inputRows, outputRows) {
		t.Errorf("Input rows and output rows do not equal: %v -> %v", inputRows, outputRows)
	}

	ensureConnValid(t, conn)
}

func TestConnCopyFromTSVector(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	pgxtest.SkipCockroachDB(t, conn, "CockroachDB handles tsvector escaping differently")

	tx, err := conn.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `create temporary table tmp_tsv (id int, t tsvector)`)
	require.NoError(t, err)

	inputRows := [][]any{
		// Text format: core functionality.
		{1, `'a':1A 'cat':5 'fat':2B,4C`}, // Multiple lexemes with positions and weights.
		{2, `'bare'`},                     // Single lexeme with no positions.
		{3, `'multi':1,2,3,4,5`},          // Multiple positions (default weight D).
		{4, `'test':1A,2B,3C,4D`},         // All four weights on one lexeme.
		{5, `'word':1D`},                  // Explicit weight D (normalizes to no suffix).
		{6, `'high':16383A`},              // High position number (near 14-bit max).

		// Text format: escaping.
		{7, `'don''t'`}, // Quote escaping (doubled single quote).
		{8, `'don\'t'`}, // Quote escaping (backslash).
		{9, `'ab\\c'`},  // Backslash in lexeme.
		{10, `'\ foo'`}, // Escaped space.

		// Text format: special characters.
		{11, `'café' 'naïve'`}, // Unicode lexemes.
		{12, `'a:b' 'c,d'`},    // Delimiter-like characters (colon, comma).

		// Struct format: tests binary encoding path.
		{13, pgtype.TSVector{
			Lexemes: []pgtype.TSVectorLexeme{
				{Word: "alpha", Positions: []pgtype.TSVectorPosition{{Position: 1, Weight: pgtype.TSVectorWeightA}}},
				{Word: "beta", Positions: []pgtype.TSVectorPosition{{Position: 2, Weight: pgtype.TSVectorWeightB}}},
				{Word: "gamma", Positions: nil},
			},
			Valid: true,
		}},
		{14, pgtype.TSVector{Valid: true}}, // Empty valid tsvector (no lexemes).

		// NULL handling.
		{15, pgtype.TSVector{Valid: false}}, // Invalid (NULL) TSVector struct.
		{16, nil},                           // Nil value.
	}

	copyCount, err := conn.CopyFrom(ctx, pgx.Identifier{"tmp_tsv"}, []string{"id", "t"}, pgx.CopyFromRows(inputRows))
	require.NoError(t, err)
	require.EqualValues(t, len(inputRows), copyCount)

	rows, err := conn.Query(ctx, "select id, t::text from tmp_tsv order by id nulls last")
	require.NoError(t, err)

	var outputRows [][]any
	for rows.Next() {
		row, err := rows.Values()
		require.NoError(t, err)
		outputRows = append(outputRows, row)
	}
	require.NoError(t, rows.Err())

	expectedOutputRows := [][]any{
		// Text format: core functionality.
		{int32(1), `'a':1A 'cat':5 'fat':2B,4C`},
		{int32(2), `'bare'`},
		{int32(3), `'multi':1,2,3,4,5`},
		{int32(4), `'test':1A,2B,3C,4`},
		{int32(5), `'word':1`},
		{int32(6), `'high':16383A`},

		// Text format: escaping.
		{int32(7), `'don''t'`},
		{int32(8), `'don''t'`},
		{int32(9), `'ab\\c'`},
		{int32(10), `' foo'`},

		// Text format: special characters.
		{int32(11), `'café' 'naïve'`},
		{int32(12), `'a:b' 'c,d'`},

		// Struct format.
		{int32(13), `'alpha':1A 'beta':2B 'gamma'`},
		{int32(14), ``},

		// NULL handling.
		{int32(15), nil},
		{int32(16), nil},
	}
	require.Equal(t, expectedOutputRows, outputRows)

	ensureConnValid(t, conn)
}

type clientFailSource struct {
	count int
	err   error
}

func (cfs *clientFailSource) Next() bool {
	cfs.count++
	return cfs.count < 100
}

func (cfs *clientFailSource) Values() ([]any, error) {
	if cfs.count == 3 {
		cfs.err = fmt.Errorf("client error")
		return nil, cfs.err
	}
	return []any{make([]byte, 100_000)}, nil
}

func (cfs *clientFailSource) Err() error {
	return cfs.err
}

func TestConnCopyFromFailServerSideMidway(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	mustExec(t, conn, `create temporary table foo(
		a int4,
		b varchar not null
	)`)

	inputRows := [][]any{
		{int32(1), "abc"},
		{int32(2), nil}, // this row should trigger a failure
		{int32(3), "def"},
	}

	copyCount, err := conn.CopyFrom(ctx, pgx.Identifier{"foo"}, []string{"a", "b"}, pgx.CopyFromRows(inputRows))
	if err == nil {
		t.Errorf("Expected CopyFrom return error, but it did not")
	}
	if _, ok := err.(*pgconn.PgError); !ok {
		t.Errorf("Expected CopyFrom return pgx.PgError, but instead it returned: %v", err)
	}
	if copyCount != 0 {
		t.Errorf("Expected CopyFrom to return 0 copied rows, but got %d", copyCount)
	}

	rows, err := conn.Query(ctx, "select * from foo")
	if err != nil {
		t.Errorf("Unexpected error for Query: %v", err)
	}

	var outputRows [][]any
	for rows.Next() {
		row, err := rows.Values()
		if err != nil {
			t.Errorf("Unexpected error for rows.Values(): %v", err)
		}
		outputRows = append(outputRows, row)
	}

	if rows.Err() != nil {
		t.Errorf("Unexpected error for rows.Err(): %v", rows.Err())
	}

	if len(outputRows) != 0 {
		t.Errorf("Expected 0 rows, but got %v", outputRows)
	}

	mustExec(t, conn, "truncate foo")

	ensureConnValid(t, conn)
}

type failSource struct {
	count int
}

func (fs *failSource) Next() bool {
	time.Sleep(time.Millisecond * 100)
	fs.count++
	return fs.count < 100
}

func (fs *failSource) Values() ([]any, error) {
	if fs.count == 3 {
		return []any{nil}, nil
	}
	return []any{make([]byte, 100_000)}, nil
}

func (fs *failSource) Err() error {
	return nil
}

func TestConnCopyFromFailServerSideMidwayAbortsWithoutWaiting(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	pgxtest.SkipCockroachDB(t, conn, "Server copy error does not fail fast")

	mustExec(t, conn, `create temporary table foo(
		a bytea not null
	)`)

	startTime := time.Now()

	copyCount, err := conn.CopyFrom(ctx, pgx.Identifier{"foo"}, []string{"a"}, &failSource{})
	if err == nil {
		t.Errorf("Expected CopyFrom return error, but it did not")
	}
	if _, ok := err.(*pgconn.PgError); !ok {
		t.Errorf("Expected CopyFrom return pgx.PgError, but instead it returned: %v", err)
	}
	if copyCount != 0 {
		t.Errorf("Expected CopyFrom to return 0 copied rows, but got %d", copyCount)
	}

	endTime := time.Now()
	copyTime := endTime.Sub(startTime)
	if copyTime > time.Second {
		t.Errorf("Failing CopyFrom shouldn't have taken so long: %v", copyTime)
	}

	rows, err := conn.Query(ctx, "select * from foo")
	if err != nil {
		t.Errorf("Unexpected error for Query: %v", err)
	}

	var outputRows [][]any
	for rows.Next() {
		row, err := rows.Values()
		if err != nil {
			t.Errorf("Unexpected error for rows.Values(): %v", err)
		}
		outputRows = append(outputRows, row)
	}

	if rows.Err() != nil {
		t.Errorf("Unexpected error for rows.Err(): %v", rows.Err())
	}

	if len(outputRows) != 0 {
		t.Errorf("Expected 0 rows, but got %v", outputRows)
	}

	ensureConnValid(t, conn)
}

type slowFailRaceSource struct {
	count int
}

func (fs *slowFailRaceSource) Next() bool {
	time.Sleep(time.Millisecond)
	fs.count++
	return fs.count < 1000
}

func (fs *slowFailRaceSource) Values() ([]any, error) {
	if fs.count == 500 {
		return []any{nil, nil}, nil
	}
	return []any{1, make([]byte, 1000)}, nil
}

func (fs *slowFailRaceSource) Err() error {
	return nil
}

func TestConnCopyFromSlowFailRace(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	mustExec(t, conn, `create temporary table foo(
		a int not null,
		b bytea not null
	)`)

	copyCount, err := conn.CopyFrom(ctx, pgx.Identifier{"foo"}, []string{"a", "b"}, &slowFailRaceSource{})
	if err == nil {
		t.Errorf("Expected CopyFrom return error, but it did not")
	}
	if _, ok := err.(*pgconn.PgError); !ok {
		t.Errorf("Expected CopyFrom return pgx.PgError, but instead it returned: %v", err)
	}
	if copyCount != 0 {
		t.Errorf("Expected CopyFrom to return 0 copied rows, but got %d", copyCount)
	}

	ensureConnValid(t, conn)
}

func TestConnCopyFromCopyFromSourceErrorMidway(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	mustExec(t, conn, `create temporary table foo(
		a bytea not null
	)`)

	copyCount, err := conn.CopyFrom(ctx, pgx.Identifier{"foo"}, []string{"a"}, &clientFailSource{})
	if err == nil {
		t.Errorf("Expected CopyFrom return error, but it did not")
	}
	if copyCount != 0 {
		t.Errorf("Expected CopyFrom to return 0 copied rows, but got %d", copyCount)
	}

	rows, err := conn.Query(ctx, "select * from foo")
	if err != nil {
		t.Errorf("Unexpected error for Query: %v", err)
	}

	var outputRows [][]any
	for rows.Next() {
		row, err := rows.Values()
		if err != nil {
			t.Errorf("Unexpected error for rows.Values(): %v", err)
		}
		outputRows = append(outputRows, row)
	}

	if rows.Err() != nil {
		t.Errorf("Unexpected error for rows.Err(): %v", rows.Err())
	}

	if len(outputRows) != 0 {
		t.Errorf("Expected 0 rows, but got %v", len(outputRows))
	}

	ensureConnValid(t, conn)
}

type clientFinalErrSource struct {
	count int
}

func (cfs *clientFinalErrSource) Next() bool {
	cfs.count++
	return cfs.count < 5
}

func (cfs *clientFinalErrSource) Values() ([]any, error) {
	return []any{make([]byte, 100_000)}, nil
}

func (cfs *clientFinalErrSource) Err() error {
	return fmt.Errorf("final error")
}

func TestConnCopyFromCopyFromSourceErrorEnd(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	mustExec(t, conn, `create temporary table foo(
		a bytea not null
	)`)

	copyCount, err := conn.CopyFrom(ctx, pgx.Identifier{"foo"}, []string{"a"}, &clientFinalErrSource{})
	if err == nil {
		t.Errorf("Expected CopyFrom return error, but it did not")
	}
	if copyCount != 0 {
		t.Errorf("Expected CopyFrom to return 0 copied rows, but got %d", copyCount)
	}

	rows, err := conn.Query(ctx, "select * from foo")
	if err != nil {
		t.Errorf("Unexpected error for Query: %v", err)
	}

	var outputRows [][]any
	for rows.Next() {
		row, err := rows.Values()
		if err != nil {
			t.Errorf("Unexpected error for rows.Values(): %v", err)
		}
		outputRows = append(outputRows, row)
	}

	if rows.Err() != nil {
		t.Errorf("Unexpected error for rows.Err(): %v", rows.Err())
	}

	if len(outputRows) != 0 {
		t.Errorf("Expected 0 rows, but got %v", outputRows)
	}

	ensureConnValid(t, conn)
}

func TestConnCopyFromAutomaticStringConversion(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	mustExec(t, conn, `create temporary table foo(
		a int8
	)`)

	inputRows := [][]any{
		{"42"},
		{"7"},
		{8},
	}

	copyCount, err := conn.CopyFrom(ctx, pgx.Identifier{"foo"}, []string{"a"}, pgx.CopyFromRows(inputRows))
	require.NoError(t, err)
	require.EqualValues(t, len(inputRows), copyCount)

	rows, _ := conn.Query(ctx, "select * from foo")
	nums, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	require.NoError(t, err)

	require.Equal(t, []int64{42, 7, 8}, nums)

	ensureConnValid(t, conn)
}

// https://github.com/jackc/pgx/discussions/1891
func TestConnCopyFromAutomaticStringConversionArray(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	mustExec(t, conn, `create temporary table foo(
		a numeric[]
	)`)

	inputRows := [][]any{
		{[]string{"42"}},
		{[]string{"7"}},
		{[]string{"8", "9"}},
		{[][]string{{"10", "11"}, {"12", "13"}}},
	}

	copyCount, err := conn.CopyFrom(ctx, pgx.Identifier{"foo"}, []string{"a"}, pgx.CopyFromRows(inputRows))
	require.NoError(t, err)
	require.EqualValues(t, len(inputRows), copyCount)

	// Test reads as int64 and flattened array for simplicity.
	rows, _ := conn.Query(ctx, "select * from foo")
	nums, err := pgx.CollectRows(rows, pgx.RowTo[[]int64])
	require.NoError(t, err)
	require.Equal(t, [][]int64{{42}, {7}, {8, 9}, {10, 11, 12, 13}}, nums)

	ensureConnValid(t, conn)
}

func TestCopyFromFunc(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	mustExec(t, conn, `create temporary table foo(
		a int
	)`)

	dataCh := make(chan int, 1)

	const channelItems = 10
	go func() {
		for i := range channelItems {
			dataCh <- i
		}
		close(dataCh)
	}()

	copyCount, err := conn.CopyFrom(context.Background(), pgx.Identifier{"foo"}, []string{"a"},
		pgx.CopyFromFunc(func() ([]any, error) {
			v, ok := <-dataCh
			if !ok {
				return nil, nil
			}
			return []any{v}, nil
		}))

	require.ErrorIs(t, err, nil)
	require.EqualValues(t, channelItems, copyCount)

	rows, err := conn.Query(context.Background(), "select * from foo order by a")
	require.NoError(t, err)
	nums, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	require.NoError(t, err)
	require.Equal(t, []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, nums)

	// simulate a failure
	copyCount, err = conn.CopyFrom(context.Background(), pgx.Identifier{"foo"}, []string{"a"},
		pgx.CopyFromFunc(func() func() ([]any, error) {
			x := 9
			return func() ([]any, error) {
				x++
				if x > 100 {
					return nil, fmt.Errorf("simulated error")
				}
				return []any{x}, nil
			}
		}()))
	require.NotErrorIs(t, err, nil)
	require.EqualValues(t, 0, copyCount) // no change, due to error

	ensureConnValid(t, conn)
}
