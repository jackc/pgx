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
			sql:          "select @Abc::int, @b_4::text",
			namedArgs:    pgx.NamedArgs{"Abc": int32(42), "b_4": "foo"},
			expectedSQL:  "select $1::int, $2::text",
			expectedArgs: []any{int32(42), "foo"},
		},
		{
			sql:          "at end @",
			namedArgs:    pgx.NamedArgs{"a": int32(42), "b": "foo"},
			expectedSQL:  "at end @",
			expectedArgs: []any{},
		},
		{
			sql:          "ignores without letter after @ foo bar",
			namedArgs:    pgx.NamedArgs{"a": int32(42), "b": "foo"},
			expectedSQL:  "ignores without letter after @ foo bar",
			expectedArgs: []any{},
		},
		{
			sql:          "name must start with letter @1 foo bar",
			namedArgs:    pgx.NamedArgs{"a": int32(42), "b": "foo"},
			expectedSQL:  "name must start with letter @1 foo bar",
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

		// test comments and quotes
	} {
		sql, args, err := tt.namedArgs.RewriteQuery(context.Background(), nil, tt.sql, tt.args)
		require.NoError(t, err)
		assert.Equalf(t, tt.expectedSQL, sql, "%d", i)
		assert.Equalf(t, tt.expectedArgs, args, "%d", i)
	}
}
