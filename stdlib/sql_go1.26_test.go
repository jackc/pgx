//go:build go1.26

package stdlib_test

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jackc/pgx/v5/pgtype"
)

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
				Valid: true},
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
			r)

		var equal bool
		err = db.QueryRow("select int4range(1, 5) = $1::int4range", r).Scan(&equal)
		require.NoError(t, err)
		require.Equal(t, true, equal)

		err = db.QueryRow("select null::int4range").Scan(&r)
		require.NoError(t, err)
		assert.Equal(t, pgtype.Range[pgtype.Int4]{}, r)
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
			r)

		var equal bool
		err = db.QueryRow("select int4multirange(int4range(1, 5), int4range(7,9)) = $1::int4multirange", r).Scan(&equal)
		require.NoError(t, err)
		require.Equal(t, true, equal)

		err = db.QueryRow("select null::int4multirange").Scan(&r)
		require.NoError(t, err)
		require.Nil(t, r)
	})
}
