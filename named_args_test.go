package pgx_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNamedArgsRewriteQuery(t *testing.T) {
	t.Parallel()

	for i, tt := range []struct {
		sql          string
		args         []any
		namedArgs    pgx.NamedArgs
		expectedSQL  string
		expectedArgs []any
	}{
		{
			sql:          "select * from users where id = @id",
			namedArgs:    pgx.NamedArgs{"id": int32(42)},
			expectedSQL:  "select * from users where id = $1",
			expectedArgs: []any{int32(42)},
		},
		{
			sql:          "select * from t where foo < @abc and baz = @def and bar < @abc",
			namedArgs:    pgx.NamedArgs{"abc": int32(42), "def": int32(1)},
			expectedSQL:  "select * from t where foo < $1 and baz = $2 and bar < $1",
			expectedArgs: []any{int32(42), int32(1)},
		},
		{
			sql:          "select @a::int, @b::text",
			namedArgs:    pgx.NamedArgs{"a": int32(42), "b": "foo"},
			expectedSQL:  "select $1::int, $2::text",
			expectedArgs: []any{int32(42), "foo"},
		},
		{
			sql:          "select @Abc::int, @b_4::text, @_c::int",
			namedArgs:    pgx.NamedArgs{"Abc": int32(42), "b_4": "foo", "_c": int32(1)},
			expectedSQL:  "select $1::int, $2::text, $3::int",
			expectedArgs: []any{int32(42), "foo", int32(1)},
		},
		{
			sql:          "at end @",
			namedArgs:    pgx.NamedArgs{"a": int32(42), "b": "foo"},
			expectedSQL:  "at end @",
			expectedArgs: []any{},
		},
		{
			sql:          "ignores without valid character after @ foo bar",
			namedArgs:    pgx.NamedArgs{"a": int32(42), "b": "foo"},
			expectedSQL:  "ignores without valid character after @ foo bar",
			expectedArgs: []any{},
		},
		{
			sql:          "name cannot start with number @1 foo bar",
			namedArgs:    pgx.NamedArgs{"a": int32(42), "b": "foo"},
			expectedSQL:  "name cannot start with number @1 foo bar",
			expectedArgs: []any{},
		},
		{
			sql:          `select *, '@foo' as "@bar" from users where id = @id`,
			namedArgs:    pgx.NamedArgs{"id": int32(42)},
			expectedSQL:  `select *, '@foo' as "@bar" from users where id = $1`,
			expectedArgs: []any{int32(42)},
		},
		{
			sql: `select * -- @foo
			from users -- @single line comments
			where id = @id;`,
			namedArgs: pgx.NamedArgs{"id": int32(42)},
			expectedSQL: `select * -- @foo
			from users -- @single line comments
			where id = $1;`,
			expectedArgs: []any{int32(42)},
		},
		{
			sql: `select * /* @multi line
			@comment
			*/
			/* /* with @nesting */ */
			from users
			where id = @id;`,
			namedArgs: pgx.NamedArgs{"id": int32(42)},
			expectedSQL: `select * /* @multi line
			@comment
			*/
			/* /* with @nesting */ */
			from users
			where id = $1;`,
			expectedArgs: []any{int32(42)},
		},
		{
			sql:          "extra provided argument",
			namedArgs:    pgx.NamedArgs{"extra": int32(1)},
			expectedSQL:  "extra provided argument",
			expectedArgs: []any{},
		},
		{
			sql:          "@missing argument",
			namedArgs:    pgx.NamedArgs{},
			expectedSQL:  "$1 argument",
			expectedArgs: []any{nil},
		},

		// test comments and quotes
	} {
		sql, args, err := tt.namedArgs.RewriteQuery(context.Background(), nil, tt.sql, tt.args)
		require.NoError(t, err)
		assert.Equalf(t, tt.expectedSQL, sql, "%d", i)
		assert.Equalf(t, tt.expectedArgs, args, "%d", i)
	}
}

func TestStrictNamedArgsRewriteQuery(t *testing.T) {
	t.Parallel()

	for i, tt := range []struct {
		sql             string
		namedArgs       pgx.StrictNamedArgs
		expectedSQL     string
		expectedArgs    []any
		isExpectedError bool
	}{
		{
			sql:             "no arguments",
			namedArgs:       pgx.StrictNamedArgs{},
			expectedSQL:     "no arguments",
			expectedArgs:    []any{},
			isExpectedError: false,
		},
		{
			sql:             "@all @matches",
			namedArgs:       pgx.StrictNamedArgs{"all": int32(1), "matches": int32(2)},
			expectedSQL:     "$1 $2",
			expectedArgs:    []any{int32(1), int32(2)},
			isExpectedError: false,
		},
		{
			sql:             "extra provided argument",
			namedArgs:       pgx.StrictNamedArgs{"extra": int32(1)},
			isExpectedError: true,
		},
		{
			sql:             "@missing argument",
			namedArgs:       pgx.StrictNamedArgs{},
			isExpectedError: true,
		},
	} {
		sql, args, err := tt.namedArgs.RewriteQuery(context.Background(), nil, tt.sql, nil)
		if tt.isExpectedError {
			assert.Errorf(t, err, "%d", i)
		} else {
			require.NoErrorf(t, err, "%d", i)
			assert.Equalf(t, tt.expectedSQL, sql, "%d", i)
			assert.Equalf(t, tt.expectedArgs, args, "%d", i)
		}
	}
}
