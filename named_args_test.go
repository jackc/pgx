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

func TestStructArgs(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name         string
		input        any
		sql          string
		expectedSQL  string
		expectedArgs []any
		expectError  bool
	}{
		{
			name: "basic",
			input: struct {
				ID   int    `db:"id"`
				Name string `db:"name,omitempty"`
				Skip string `db:"-"`
			}{ID: 42, Name: "x", Skip: "ignored"},
			sql:          "select * from t where id=@id and name=@name",
			expectedSQL:  "select * from t where id=$1 and name=$2",
			expectedArgs: []any{42, "x"},
		},
		{
			name: "pointer",
			input: func() any {
				type S struct {
					ID int `db:"id"`
				}
				return &S{ID: 7}
			}(),
			sql:          "select * from t where id=@id",
			expectedSQL:  "select * from t where id=$1",
			expectedArgs: []any{7},
		},
		{
			name: "unexported fields omitted (missing placeholders become nil)",
			input: struct {
				id int `db:"id"`
				ID int `db:"ID"`
			}{id: 1, ID: 2},
			sql:          "select * from t where ID=@ID and id=@id",
			expectedSQL:  "select * from t where ID=$1 and id=$2",
			expectedArgs: []any{2, nil},
		},
		{
			name: "missing db tag falls back to field name",
			input: struct {
				ID int
			}{ID: 9},
			sql:          "select * from t where ID=@ID",
			expectedSQL:  "select * from t where ID=$1",
			expectedArgs: []any{9},
		},
		{
			name: "duplicate keys error",
			input: struct {
				A int `db:"x"`
				B int `db:"x"`
			}{A: 1, B: 2},
			sql:         "select * from t where x=@x",
			expectError: true,
		},
		{
			name: "nil pointer returns error",
			input: func() any {
				type S struct {
					ID int `db:"id"`
				}
				var s *S
				return s
			}(),
			sql:         "select * from t where id=@id",
			expectError: true,
		},
		{
			name:        "non struct returns error",
			input:       42,
			sql:         "select * from t where id=@id",
			expectError: true,
		},
		{
			name:        "nil input returns error",
			input:       nil,
			sql:         "select * from t where id=@id",
			expectError: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			qr := pgx.StructArgs(tt.input)
			sql, args, err := qr.RewriteQuery(context.Background(), nil, tt.sql, nil)
			if tt.expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expectedSQL, sql)
			assert.EqualValues(t, tt.expectedArgs, args)
		})
	}
}

func TestStrictStructArgs(t *testing.T) {
	t.Parallel()

	type MyInt int

	for _, tt := range []struct {
		name         string
		input        any
		sql          string
		expectedSQL  string
		expectedArgs []any
		expectError  bool
	}{
		{
			name: "fallback to field name without db tag",
			input: struct {
				ID int
			}{ID: 1},
			sql:          "select * from t where ID=@ID",
			expectedSQL:  "select * from t where ID=$1",
			expectedArgs: []any{1},
		},
		{
			name: "empty db tag errors",
			input: struct {
				ID int `db:","`
			}{ID: 1},
			sql:         "select * from t where ID=@ID",
			expectError: true,
		},
		{
			name: "duplicate keys error",
			input: struct {
				A int `db:"x"`
				B int `db:"x"`
			}{A: 1, B: 2},
			sql:         "select * from t where x=@x",
			expectError: true,
		},
		{
			name: "skips anonymous embedded structs without flattening",
			input: func() any {
				type Embedded struct {
					ID int `db:"id"`
				}
				type S struct {
					Embedded
					Name string `db:"name"`
				}
				return S{Embedded: Embedded{ID: 1}, Name: "x"}
			}(),
			sql:         "select * from t where name=@name and id=@id",
			expectError: true,
		},
		{
			name: "anonymous embedded non-struct still requires tag in strict mode",
			input: func() any {
				type S struct {
					MyInt
				}
				return S{MyInt: 1}
			}(),
			sql:          "select * from t where MyInt=@MyInt",
			expectedSQL:  "select * from t where MyInt=$1",
			expectedArgs: []any{MyInt(1)},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			qr := pgx.StrictStructArgs(tt.input)
			sql, args, err := qr.RewriteQuery(context.Background(), nil, tt.sql, nil)
			if tt.expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expectedSQL, sql)
			assert.EqualValues(t, tt.expectedArgs, args)
		})
	}
}
