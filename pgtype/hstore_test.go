package pgtype_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgtype/testutil"
)

func isExpectedEqMapStringString(a interface{}) func(interface{}) bool {
	return func(v interface{}) bool {
		am := a.(map[string]string)
		vm := v.(map[string]string)

		if len(am) != len(vm) {
			return false
		}

		for k, v := range am {
			if vm[k] != v {
				return false
			}
		}

		return true
	}
}

func isExpectedEqMapStringPointerString(a interface{}) func(interface{}) bool {
	return func(v interface{}) bool {
		am := a.(map[string]*string)
		vm := v.(map[string]*string)

		if len(am) != len(vm) {
			return false
		}

		for k, v := range am {
			if (vm[k] == nil) != (v == nil) {
				return false
			}

			if v != nil && *vm[k] != *v {
				return false
			}
		}

		return true
	}
}

func TestHstoreCodec(t *testing.T) {
	conn := testutil.MustConnectPgx(t)
	defer testutil.MustCloseContext(t, conn)

	var hstoreOID uint32
	err := conn.QueryRow(context.Background(), `select oid from pg_type where typname = 'hstore'`).Scan(&hstoreOID)
	if err != nil {
		t.Skipf("Skipping: cannot find hstore OID")
	}

	conn.TypeMap().RegisterType(&pgtype.Type{Name: "hstore", OID: hstoreOID, Codec: pgtype.HstoreCodec{}})

	formats := []struct {
		name string
		code int16
	}{
		{name: "TextFormat", code: pgx.TextFormatCode},
		{name: "BinaryFormat", code: pgx.BinaryFormatCode},
	}

	fs := func(s string) *string {
		return &s
	}

	tests := []testutil.TranscodeTestCase{
		{
			map[string]string{},
			new(map[string]string),
			isExpectedEqMapStringString(map[string]string{}),
		},
		{
			map[string]string{"foo": "", "bar": "", "baz": "123"},
			new(map[string]string),
			isExpectedEqMapStringString(map[string]string{"foo": "", "bar": "", "baz": "123"}),
		},
		{
			map[string]string{"NULL": "bar"},
			new(map[string]string),
			isExpectedEqMapStringString(map[string]string{"NULL": "bar"}),
		},
		{
			map[string]string{"bar": "NULL"},
			new(map[string]string),
			isExpectedEqMapStringString(map[string]string{"bar": "NULL"}),
		},
		{
			map[string]string{"": "foo"},
			new(map[string]string),
			isExpectedEqMapStringString(map[string]string{"": "foo"}),
		},
		{
			map[string]*string{},
			new(map[string]*string),
			isExpectedEqMapStringPointerString(map[string]*string{}),
		},
		{
			map[string]*string{"foo": fs("bar"), "baq": fs("quz")},
			new(map[string]*string),
			isExpectedEqMapStringPointerString(map[string]*string{"foo": fs("bar"), "baq": fs("quz")}),
		},
		{
			map[string]*string{"foo": nil, "baq": fs("quz")},
			new(map[string]*string),
			isExpectedEqMapStringPointerString(map[string]*string{"foo": nil, "baq": fs("quz")}),
		},
		{nil, new(*map[string]string), isExpectedEq((*map[string]string)(nil))},
		{nil, new(*map[string]*string), isExpectedEq((*map[string]*string)(nil))},
		{nil, new(*pgtype.Hstore), isExpectedEq((*pgtype.Hstore)(nil))},
	}

	specialStrings := []string{
		`"`,
		`'`,
		`\`,
		`\\`,
		`=>`,
		` `,
		`\ / / \\ => " ' " '`,
	}
	for _, s := range specialStrings {
		// Special key values

		// at beginning
		tests = append(tests, testutil.TranscodeTestCase{
			map[string]string{s + "foo": "bar"},
			new(map[string]string),
			isExpectedEqMapStringString(map[string]string{s + "foo": "bar"}),
		})
		// in middle
		tests = append(tests, testutil.TranscodeTestCase{
			map[string]string{"foo" + s + "bar": "bar"},
			new(map[string]string),
			isExpectedEqMapStringString(map[string]string{"foo" + s + "bar": "bar"}),
		})
		// at end
		tests = append(tests, testutil.TranscodeTestCase{
			map[string]string{"foo" + s: "bar"},
			new(map[string]string),
			isExpectedEqMapStringString(map[string]string{"foo" + s: "bar"}),
		})
		// is key
		tests = append(tests, testutil.TranscodeTestCase{
			map[string]string{s: "bar"},
			new(map[string]string),
			isExpectedEqMapStringString(map[string]string{s: "bar"}),
		})

		// Special value values

		// at beginning
		tests = append(tests, testutil.TranscodeTestCase{
			map[string]string{"foo": s + "bar"},
			new(map[string]string),
			isExpectedEqMapStringString(map[string]string{"foo": s + "bar"}),
		})
		// in middle
		tests = append(tests, testutil.TranscodeTestCase{
			map[string]string{"foo": "foo" + s + "bar"},
			new(map[string]string),
			isExpectedEqMapStringString(map[string]string{"foo": "foo" + s + "bar"}),
		})
		// at end
		tests = append(tests, testutil.TranscodeTestCase{
			map[string]string{"foo": "foo" + s},
			new(map[string]string),
			isExpectedEqMapStringString(map[string]string{"foo": "foo" + s}),
		})
		// is key
		tests = append(tests, testutil.TranscodeTestCase{
			map[string]string{"foo": s},
			new(map[string]string),
			isExpectedEqMapStringString(map[string]string{"foo": s}),
		})
	}

	for _, format := range formats {
		testutil.RunTranscodeTestsFormat(t, "hstore", tests, conn, format.name, format.code)
	}
}
