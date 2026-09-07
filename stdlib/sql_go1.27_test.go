//go:build go1.27

package stdlib_test

import (
	"database/sql"
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/stdlib"
)

func testWithKnownOIDQueryExecModes(t *testing.T, f func(t *testing.T, db *sql.DB)) {
	for _, mode := range []pgx.QueryExecMode{
		pgx.QueryExecModeCacheStatement,
		pgx.QueryExecModeCacheDescribe,
		pgx.QueryExecModeDescribeExec,
	} {
		t.Run(mode.String(),
			func(t *testing.T) {
				config, err := pgx.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
				require.NoError(t, err)

				config.DefaultQueryExecMode = mode
				db := stdlib.OpenDB(*config)
				defer func() {
					err := db.Close()
					require.NoError(t, err)
				}()

				f(t, db)

				ensureDBValid(t, db)
			},
		)
	}
}

func TestGoArray(t *testing.T) {
	testWithAllQueryExecModes(t, func(t *testing.T, db *sql.DB) {
		var names []string

		err := db.QueryRow("select array['John', 'Jane']::text[]").Scan(&names)
		require.NoError(t, err)
		require.Equal(t, []string{"John", "Jane"}, names)

		var n int
		err = db.QueryRow("select cardinality($1::text[])", names).Scan(&n)
		require.NoError(t, err)
		require.EqualValues(t, 2, n)

		err = db.QueryRow("select null::text[]").Scan(&names)
		require.NoError(t, err)
		require.Nil(t, names)
	})
}

func TestGoArrayOfDriverValuer(t *testing.T) {
	// Because []sql.NullString is not a registered type on the connection, it will only work with known OIDs.
	testWithKnownOIDQueryExecModes(t, func(t *testing.T, db *sql.DB) {
		var names []sql.NullString

		err := db.QueryRow("select array['John', null, 'Jane']::text[]").Scan(&names)
		require.NoError(t, err)
		require.Equal(t, []sql.NullString{{String: "John", Valid: true}, {}, {String: "Jane", Valid: true}}, names)

		var n int
		err = db.QueryRow("select cardinality($1::text[])", names).Scan(&n)
		require.NoError(t, err)
		require.EqualValues(t, 3, n)

		err = db.QueryRow("select null::text[]").Scan(&names)
		require.NoError(t, err)
		require.Nil(t, names)
	})
}

func TestPGTypeFlatArray(t *testing.T) {
	testWithAllQueryExecModes(t, func(t *testing.T, db *sql.DB) {
		var names pgtype.FlatArray[string]

		err := db.QueryRow("select array['John', 'Jane']::text[]").Scan(&names)
		require.NoError(t, err)
		require.Equal(t, pgtype.FlatArray[string]{"John", "Jane"}, names)

		var n int
		err = db.QueryRow("select cardinality($1::text[])", names).Scan(&n)
		require.NoError(t, err)
		require.EqualValues(t, 2, n)

		err = db.QueryRow("select null::text[]").Scan(&names)
		require.NoError(t, err)
		require.Nil(t, names)
	})
}

func TestPGTypeArray(t *testing.T) {
	testWithAllQueryExecModes(t, func(t *testing.T, db *sql.DB) {
		skipCockroachDB(t, db, "Server does not support nested arrays")

		var matrix pgtype.Array[int64]

		err := db.QueryRow("select '{{1,2,3},{4,5,6}}'::bigint[]").Scan(&matrix)
		require.NoError(t, err)
		require.Equal(t,
			pgtype.Array[int64]{
				Elements: []int64{1, 2, 3, 4, 5, 6},
				Dims: []pgtype.ArrayDimension{
					{Length: 2, LowerBound: 1},
					{Length: 3, LowerBound: 1},
				},
				Valid: true,
			},
			matrix)

		var equal bool
		err = db.QueryRow("select '{{1,2,3},{4,5,6}}'::bigint[] = $1::bigint[]", matrix).Scan(&equal)
		require.NoError(t, err)
		require.Equal(t, true, equal)

		err = db.QueryRow("select null::bigint[]").Scan(&matrix)
		require.NoError(t, err)
		assert.Equal(t, pgtype.Array[int64]{Elements: nil, Dims: nil, Valid: false}, matrix)
	})
}

func TestConnQueryPGTypeRange(t *testing.T) {
	testWithAllQueryExecModes(t, func(t *testing.T, db *sql.DB) {
		skipCockroachDB(t, db, "Server does not support int4range")

		var r pgtype.Range[pgtype.Int4]
		err := db.QueryRow("select int4range(1, 5)").Scan(&r)
		require.NoError(t, err)
		assert.Equal(
			t,
			pgtype.Range[pgtype.Int4]{
				Lower:     pgtype.Int4{Int32: 1, Valid: true},
				Upper:     pgtype.Int4{Int32: 5, Valid: true},
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Exclusive,
				Valid:     true,
			},
			r,
		)

		var equal bool
		err = db.QueryRow("select int4range(1, 5) = $1::int4range", r).Scan(&equal)
		require.NoError(t, err)
		require.Equal(t, true, equal)

		err = db.QueryRow("select null::int4range").Scan(&r)
		require.NoError(t, err)
		assert.Equal(t, pgtype.Range[pgtype.Int4]{}, r)
	})
}

// Boolean stringification must agree in binary and text formats.
func TestScanBoolToString(t *testing.T) {
	testWithAllQueryExecModes(t, func(t *testing.T, db *sql.DB) {
		var s string
		err := db.QueryRow("select true").Scan(&s)
		require.NoError(t, err)
		require.Equal(t, "true", s)

		err = db.QueryRow("select false").Scan(&s)
		require.NoError(t, err)
		require.Equal(t, "false", s)
	})
}

func TestScanInterfacePreservesDatabaseSQLValues(t *testing.T) {
	testWithAllQueryExecModes(t, func(t *testing.T, db *sql.DB) {
		for _, tt := range []struct {
			query string
			want  any
		}{
			{"select 42::int2", int64(42)},
			{"select 42::int4", int64(42)},
			{"select 42::int8", int64(42)},
			{"select 1.5::float4", float64(1.5)},
			{"select 1.25::numeric", "1.25"},
			{"select array[1,2]::int4[]", "{1,2}"},
			{"select '9007199254740993'::jsonb", []byte("9007199254740993")},
			{"select 'null'::jsonb", []byte("null")},
			{"select null::int4", nil},
		} {
			t.Run(tt.query, func(t *testing.T) {
				var got any
				err := db.QueryRow(tt.query).Scan(&got)
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)

				// Nullable interface pointers must use the same representation.
				var indirect *any
				err = db.QueryRow(tt.query).Scan(&indirect)
				require.NoError(t, err)
				if tt.want == nil {
					assert.Nil(t, indirect)
				} else {
					require.NotNil(t, indirect)
					assert.Equal(t, tt.want, *indirect)
				}
			})
		}
	})
}

func TestScanScalarPreservesDatabaseSQLConversions(t *testing.T) {
	type namedString string
	testWithAllQueryExecModes(t, func(t *testing.T, db *sql.DB) {
		for _, tt := range []struct{ query, want string }{
			{"select true", "true"},
			{"select '2026-09-07'::date", "2026-09-07T00:00:00Z"},
			{"select decode('6162', 'hex')", "ab"},
		} {
			t.Run(tt.query, func(t *testing.T) {
				var s string
				err := db.QueryRow(tt.query).Scan(&s)
				require.NoError(t, err)
				assert.Equal(t, tt.want, s)

				var b []byte
				err = db.QueryRow(tt.query).Scan(&b)
				require.NoError(t, err)
				assert.Equal(t, []byte(tt.want), b)

				var indirect *string
				err = db.QueryRow(tt.query).Scan(&indirect)
				require.NoError(t, err)
				require.NotNil(t, indirect)
				assert.Equal(t, tt.want, *indirect)

				rows, err := db.Query(tt.query)
				require.NoError(t, err)
				defer rows.Close()
				require.True(t, rows.Next())
				var raw sql.RawBytes
				err = rows.Scan(&raw)
				require.NoError(t, err)
				assert.Equal(t, sql.RawBytes(tt.want), raw)
				require.NoError(t, rows.Close())
			})
		}

		var named namedString
		err := db.QueryRow("select decode('6162', 'hex')").Scan(&named)
		require.NoError(t, err)
		assert.Equal(t, namedString("ab"), named)

		var s string
		err = db.QueryRow("select null::jsonb").Scan(&s)
		require.Error(t, err)
	})
}

type scanOnce struct {
	calls int
	err   error
}

func (s *scanOnce) Scan(any) error {
	s.calls++
	if s.calls == 1 {
		return s.err
	}
	return nil
}

func TestScanPreservesScannerError(t *testing.T) {
	testWithAllQueryExecModes(t, func(t *testing.T, db *sql.DB) {
		for _, query := range []string{"select 42::int4", "select '1'::jsonb", "select null::int4"} {
			errRejected := errors.New("value rejected by scanner")
			s := &scanOnce{err: errRejected}
			err := db.QueryRow(query).Scan(s)
			assert.ErrorIs(t, err, errRejected)
			assert.Equal(t, 1, s.calls)
		}
	})
}

func TestScanRejectsNilDestination(t *testing.T) {
	testWithAllQueryExecModes(t, func(t *testing.T, db *sql.DB) {
		for _, dest := range []any{nil, (*int64)(nil), (*string)(nil), (*[]int32)(nil), (*sql.NullString)(nil)} {
			for _, query := range []string{"select 42::int4", "select null::int4"} {
				err := db.QueryRow(query).Scan(dest)
				require.Error(t, err)
			}
		}
	})
}

func TestScanNativeByteSlices(t *testing.T) {
	testWithAllQueryExecModes(t, func(t *testing.T, db *sql.DB) {
		var a pgtype.FlatArray[byte]
		err := db.QueryRow("select array[1,2,3]::int2[]").Scan(&a)
		require.NoError(t, err)
		assert.Equal(t, pgtype.FlatArray[byte]{1, 2, 3}, a)

		backing := make([]byte, 16)
		b := pgtype.PreallocBytes(backing)
		err = db.QueryRow("select decode('6162', 'hex')").Scan(&b)
		require.NoError(t, err)
		assert.Equal(t, pgtype.PreallocBytes("ab"), b)
		assert.Same(t, &backing[0], &b[0])
	})
}

func TestConnQueryPGTypeMultirange(t *testing.T) {
	testWithAllQueryExecModes(t, func(t *testing.T, db *sql.DB) {
		skipCockroachDB(t, db, "Server does not support int4range")
		skipPostgreSQLVersionLessThan(t, db, 14)

		var r pgtype.Multirange[pgtype.Range[pgtype.Int4]]
		err := db.QueryRow("select int4multirange(int4range(1, 5), int4range(7,9))").Scan(&r)
		require.NoError(t, err)
		assert.Equal(
			t,
			pgtype.Multirange[pgtype.Range[pgtype.Int4]]{
				{
					Lower:     pgtype.Int4{Int32: 1, Valid: true},
					Upper:     pgtype.Int4{Int32: 5, Valid: true},
					LowerType: pgtype.Inclusive,
					UpperType: pgtype.Exclusive,
					Valid:     true,
				},
				{
					Lower:     pgtype.Int4{Int32: 7, Valid: true},
					Upper:     pgtype.Int4{Int32: 9, Valid: true},
					LowerType: pgtype.Inclusive,
					UpperType: pgtype.Exclusive,
					Valid:     true,
				},
			},
			r,
		)

		var equal bool
		err = db.QueryRow("select int4multirange(int4range(1, 5), int4range(7,9)) = $1::int4multirange", r).Scan(&equal)
		require.NoError(t, err)
		require.Equal(t, true, equal)

		err = db.QueryRow("select null::int4multirange").Scan(&r)
		require.NoError(t, err)
		require.Nil(t, r)
	})
}
