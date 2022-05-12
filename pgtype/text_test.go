package pgtype_test

import (
	"context"
	"testing"

	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxtest"
	"github.com/stretchr/testify/require"
)

type someFmtStringer struct{}

func (someFmtStringer) String() string {
	return "some fmt.Stringer"
}

func TestTextCodec(t *testing.T) {
	for _, pgTypeName := range []string{"text", "varchar"} {
		pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, pgTypeName, []pgxtest.ValueRoundTripTest{
			{
				pgtype.Text{String: "", Valid: true},
				new(pgtype.Text),
				isExpectedEq(pgtype.Text{String: "", Valid: true}),
			},
			{
				pgtype.Text{String: "foo", Valid: true},
				new(pgtype.Text),
				isExpectedEq(pgtype.Text{String: "foo", Valid: true}),
			},
			{nil, new(pgtype.Text), isExpectedEq(pgtype.Text{})},
			{"foo", new(string), isExpectedEq("foo")},
			{someFmtStringer{}, new(string), isExpectedEq("some fmt.Stringer")},
		})
	}
}

// name is PostgreSQL's special 63-byte data type, used for identifiers like table names.  The pg_class.relname column
// is a good example of where the name data type is used.
//
// TextCodec does not do length checking. Inputting a longer name into PostgreSQL will result in silent truncation to
// 63 bytes.
//
// Length checking would be possible with a Codec specialized for "name" but it would be perfect because a
// custom-compiled PostgreSQL could have set NAMEDATALEN to a different value rather than the default 63.
//
// So this is simply a smoke test of the name type.
func TestTextCodecName(t *testing.T) {
	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "name", []pgxtest.ValueRoundTripTest{
		{
			pgtype.Text{String: "", Valid: true},
			new(pgtype.Text),
			isExpectedEq(pgtype.Text{String: "", Valid: true}),
		},
		{
			pgtype.Text{String: "foo", Valid: true},
			new(pgtype.Text),
			isExpectedEq(pgtype.Text{String: "foo", Valid: true}),
		},
		{nil, new(pgtype.Text), isExpectedEq(pgtype.Text{})},
		{"foo", new(string), isExpectedEq("foo")},
	})
}

// Test fixed length char types like char(3)
func TestTextCodecBPChar(t *testing.T) {
	skipCockroachDB(t, "Server does not properly handle bpchar with multi-byte character")

	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "char(3)", []pgxtest.ValueRoundTripTest{
		{
			pgtype.Text{String: "a  ", Valid: true},
			new(pgtype.Text),
			isExpectedEq(pgtype.Text{String: "a  ", Valid: true}),
		},
		{nil, new(pgtype.Text), isExpectedEq(pgtype.Text{})},
		{"   ", new(string), isExpectedEq("   ")},
		{"", new(string), isExpectedEq("   ")},
		{" 嗨 ", new(string), isExpectedEq(" 嗨 ")},
	})
}

// ACLItem is used for PostgreSQL's aclitem data type. A sample aclitem
// might look like this:
//
//	postgres=arwdDxt/postgres
//
// Note, however, that because the user/role name part of an aclitem is
// an identifier, it follows all the usual formatting rules for SQL
// identifiers: if it contains spaces and other special characters,
// it should appear in double-quotes:
//
//	postgres=arwdDxt/"role with spaces"
//
// It only supports the text format.
func TestTextCodecACLItem(t *testing.T) {
	ctr := defaultConnTestRunner
	ctr.AfterConnect = func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		pgxtest.SkipCockroachDB(t, conn, "Server does not support type aclitem")
	}

	pgxtest.RunValueRoundTripTests(context.Background(), t, ctr, nil, "aclitem", []pgxtest.ValueRoundTripTest{
		{
			pgtype.Text{String: "postgres=arwdDxt/postgres", Valid: true},
			new(pgtype.Text),
			isExpectedEq(pgtype.Text{String: "postgres=arwdDxt/postgres", Valid: true}),
		},
		{pgtype.Text{}, new(pgtype.Text), isExpectedEq(pgtype.Text{})},
		{nil, new(pgtype.Text), isExpectedEq(pgtype.Text{})},
	})
}

func TestTextCodecACLItemRoleWithSpecialCharacters(t *testing.T) {
	ctr := defaultConnTestRunner
	ctr.AfterConnect = func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		pgxtest.SkipCockroachDB(t, conn, "Server does not support type aclitem")

		// The tricky test user, below, has to actually exist so that it can be used in a test
		// of aclitem formatting. It turns out aclitems cannot contain non-existing users/roles.
		roleWithSpecialCharacters := ` tricky, ' } " \ test user `

		commandTag, err := conn.Exec(ctx, `select * from pg_roles where rolname = $1`, roleWithSpecialCharacters)
		require.NoError(t, err)

		if commandTag.RowsAffected() == 0 {
			t.Skipf("Role with special characters does not exist.")
		}
	}

	pgxtest.RunValueRoundTripTests(context.Background(), t, ctr, nil, "aclitem", []pgxtest.ValueRoundTripTest{
		{
			pgtype.Text{String: `postgres=arwdDxt/" tricky, ' } "" \ test user "`, Valid: true},
			new(pgtype.Text),
			isExpectedEq(pgtype.Text{String: `postgres=arwdDxt/" tricky, ' } "" \ test user "`, Valid: true}),
		},
	})
}

func TestTextMarshalJSON(t *testing.T) {
	successfulTests := []struct {
		source pgtype.Text
		result string
	}{
		{source: pgtype.Text{String: ""}, result: "null"},
		{source: pgtype.Text{String: "a", Valid: true}, result: "\"a\""},
	}
	for i, tt := range successfulTests {
		r, err := tt.source.MarshalJSON()
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if string(r) != tt.result {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.source, tt.result, string(r))
		}
	}
}

func TestTextUnmarshalJSON(t *testing.T) {
	successfulTests := []struct {
		source string
		result pgtype.Text
	}{
		{source: "null", result: pgtype.Text{String: ""}},
		{source: "\"a\"", result: pgtype.Text{String: "a", Valid: true}},
	}
	for i, tt := range successfulTests {
		var r pgtype.Text
		err := r.UnmarshalJSON([]byte(tt.source))
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if r != tt.result {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.source, tt.result, r)
		}
	}
}
