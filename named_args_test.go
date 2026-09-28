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
			// A backslash has no meaning in a -- comment; the comment still ends at the newline.
			sql:          "select * -- C:\\path\\\nfrom users where id = @id",
			namedArgs:    pgx.NamedArgs{"id": int32(42)},
			expectedSQL:  "select * -- C:\\path\\\nfrom users where id = $1",
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

		// U+FFFD is a valid character, not the end of the query.
		{
			sql:          "select * from t where note = 'a\uFFFDb' and id = @id",
			namedArgs:    pgx.NamedArgs{"id": int32(42)},
			expectedSQL:  "select * from t where note = 'a\uFFFDb' and id = $1",
			expectedArgs: []any{int32(42)},
		},
		{
			sql:          "select * from t where id = @id -- \uFFFD\nand deleted = false",
			namedArgs:    pgx.NamedArgs{"id": int32(42)},
			expectedSQL:  "select * from t where id = $1 -- \uFFFD\nand deleted = false",
			expectedArgs: []any{int32(42)},
		},
		{
			sql:          "select * from t where id = @id /* \uFFFD */ and deleted = false",
			namedArgs:    pgx.NamedArgs{"id": int32(42)},
			expectedSQL:  "select * from t where id = $1 /* \uFFFD */ and deleted = false",
			expectedArgs: []any{int32(42)},
		},
		{
			sql:          "select * from \"a\uFFFDb\" where id = @id",
			namedArgs:    pgx.NamedArgs{"id": int32(42)},
			expectedSQL:  "select * from \"a\uFFFDb\" where id = $1",
			expectedArgs: []any{int32(42)},
		},
		{
			sql:          "select * from t where note = e'a\uFFFDb' and id = @id",
			namedArgs:    pgx.NamedArgs{"id": int32(42)},
			expectedSQL:  "select * from t where note = e'a\uFFFDb' and id = $1",
			expectedArgs: []any{int32(42)},
		},
		{
			sql:          "select @id\uFFFD, @other",
			namedArgs:    pgx.NamedArgs{"id": int32(42), "other": int32(7)},
			expectedSQL:  "select $1\uFFFD, $2",
			expectedArgs: []any{int32(42), int32(7)},
		},
		// Invalid UTF-8, unlike U+FFFD, still ends the input.
		{
			sql:          "select * from t where id = @id and note = '\xffb'",
			namedArgs:    pgx.NamedArgs{"id": int32(42)},
			expectedSQL:  "select * from t where id = $1 and note = '\xff",
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

func TestNamedArgsDollarQuotedStrings(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		sql  string
		want string
	}{
		{"anonymous", `select $$hello @id$$, @id`, `select $$hello @id$$, $1`},
		{"tagged", `select $tag_1$@missing$tag_1$, @id`, `select $tag_1$@missing$tag_1$, $1`},
		{"unicode tag", `select $世界$@missing$世界$, @id`, `select $世界$@missing$世界$, $1`},
		{"replacement character tag", `select $�$@missing$�$, @id`, `select $�$@missing$�$, $1`},
		{"opaque contents", `select $$it's " -- /* @missing \ $$, @id`, `select $$it's " -- /* @missing \ $$, $1`},
		{"different inner tags", `select $outer$$inner$@missing$inner$ $$ @missing$outer$, @id`, `select $outer$$inner$@missing$inner$ $$ @missing$outer$, $1`},
		{"case sensitive tag", `select $tag$@missing$TAG$@missing$tag$, @id`, `select $tag$@missing$TAG$@missing$tag$, $1`},
		{"multiple literals", `select @id, $$@missing$$, $tag$@id$tag$, @id`, `select $1, $$@missing$$, $tag$@id$tag$, $1`},
		{"unterminated", `select @id, $tag$@missing`, `select $1, $tag$@missing`},
		{"dollar in identifier", `select column$tag$, @id`, `select column$tag$, $1`},
		{"dollars in identifier", `select column$$, @id`, `select column$$, $1`},
		{"identifier starting with e", `select example$tag$, @id`, `select example$tag$, $1`},
		{"unicode identifier", `select 世界$tag$, @id`, `select 世界$tag$, $1`},
		{"invalid numeric tag", `select $1$ @id`, `select $1$ $1`},
		{"invalid tag character", `select $bad-tag$ @id`, `select $bad-tag$ $1`},
		{"lone dollar", `select @id, $`, `select $1, $`},
		{"quoted delimiter", `select '$$' , @id`, `select '$$' , $1`},
		{"commented delimiter", "select /* $$ */ @id -- $tag$", "select /* $$ */ $1 -- $tag$"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, rewriter := range []pgx.QueryRewriter{
				pgx.NamedArgs{"id": 42},
				pgx.StrictNamedArgs{"id": 42},
				pgx.StructArgs(struct {
					ID int `db:"id"`
				}{42}),
				pgx.StrictStructArgs(struct {
					ID int `db:"id"`
				}{42}),
			} {
				sql, args, err := rewriter.RewriteQuery(context.Background(), nil, tt.sql, nil)
				require.NoError(t, err)
				assert.Equal(t, tt.want, sql)
				assert.Equal(t, []any{42}, args)
			}
		})
	}

	t.Run("no phantom arguments", func(t *testing.T) {
		for _, rewriter := range []pgx.QueryRewriter{pgx.NamedArgs{}, pgx.StrictNamedArgs{}} {
			const query = `select $$hello @world$$`
			sql, args, err := rewriter.RewriteQuery(context.Background(), nil, query, nil)
			require.NoError(t, err)
			assert.Equal(t, query, sql)
			assert.Empty(t, args)
		}
	})

	t.Run("strict rejects argument used only in literal", func(t *testing.T) {
		_, _, err := (pgx.StrictNamedArgs{"id": 42}).RewriteQuery(context.Background(), nil, `select $$@id$$`, nil)
		require.EqualError(t, err, "argument id of StrictNamedArgs not found in sql query")
	})
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
