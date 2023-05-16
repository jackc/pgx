package pgtype_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxtest"
)

func isExpectedEqMapStringString(a any) func(any) bool {
	return func(v any) bool {
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

func isExpectedEqMapStringPointerString(a any) func(any) bool {
	return func(v any) bool {
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
	ctr := defaultConnTestRunner
	ctr.AfterConnect = func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		var hstoreOID uint32
		err := conn.QueryRow(context.Background(), `select oid from pg_type where typname = 'hstore'`).Scan(&hstoreOID)
		if err != nil {
			t.Skipf("Skipping: cannot find hstore OID")
		}

		conn.TypeMap().RegisterType(&pgtype.Type{Name: "hstore", OID: hstoreOID, Codec: pgtype.HstoreCodec{}})
	}

	fs := func(s string) *string {
		return &s
	}

	tests := []pgxtest.ValueRoundTripTest{
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
		"line1\nline2",
		"tab\tafter",
		"vtab\vafter",
		"form\\ffeed",
		"carriage\rreturn",
		"curly{}braces",
	}
	for _, s := range specialStrings {
		// Special key values

		// at beginning
		tests = append(tests, pgxtest.ValueRoundTripTest{
			map[string]string{s + "foo": "bar"},
			new(map[string]string),
			isExpectedEqMapStringString(map[string]string{s + "foo": "bar"}),
		})
		// in middle
		tests = append(tests, pgxtest.ValueRoundTripTest{
			map[string]string{"foo" + s + "bar": "bar"},
			new(map[string]string),
			isExpectedEqMapStringString(map[string]string{"foo" + s + "bar": "bar"}),
		})
		// at end
		tests = append(tests, pgxtest.ValueRoundTripTest{
			map[string]string{"foo" + s: "bar"},
			new(map[string]string),
			isExpectedEqMapStringString(map[string]string{"foo" + s: "bar"}),
		})
		// is key
		tests = append(tests, pgxtest.ValueRoundTripTest{
			map[string]string{s: "bar"},
			new(map[string]string),
			isExpectedEqMapStringString(map[string]string{s: "bar"}),
		})

		// Special value values

		// at beginning
		tests = append(tests, pgxtest.ValueRoundTripTest{
			map[string]string{"foo": s + "bar"},
			new(map[string]string),
			isExpectedEqMapStringString(map[string]string{"foo": s + "bar"}),
		})
		// in middle
		tests = append(tests, pgxtest.ValueRoundTripTest{
			map[string]string{"foo": "foo" + s + "bar"},
			new(map[string]string),
			isExpectedEqMapStringString(map[string]string{"foo": "foo" + s + "bar"}),
		})
		// at end
		tests = append(tests, pgxtest.ValueRoundTripTest{
			map[string]string{"foo": "foo" + s},
			new(map[string]string),
			isExpectedEqMapStringString(map[string]string{"foo": "foo" + s}),
		})
		// is key
		tests = append(tests, pgxtest.ValueRoundTripTest{
			map[string]string{"foo": s},
			new(map[string]string),
			isExpectedEqMapStringString(map[string]string{"foo": s}),
		})
	}

	pgxtest.RunValueRoundTripTests(context.Background(), t, ctr, pgxtest.KnownOIDQueryExecModes, "hstore", tests)

	// run the tests using pgtype.Hstore as input and output types, and test all query modes
	for i := range tests {
		var h pgtype.Hstore
		switch typedParam := tests[i].Param.(type) {
		case map[string]*string:
			h = pgtype.Hstore(typedParam)
		case map[string]string:
			if typedParam != nil {
				h = pgtype.Hstore{}
				for k, v := range typedParam {
					h[k] = fs(v)
				}
			}
		}

		tests[i].Param = h
		tests[i].Result = &pgtype.Hstore{}
		tests[i].Test = func(input any) bool {
			return reflect.DeepEqual(input, h)
		}
	}
	pgxtest.RunValueRoundTripTests(context.Background(), t, ctr, pgxtest.AllQueryExecModes, "hstore", tests)

	// run the tests again without the codec registered: uses the text protocol
	ctrWithoutCodec := defaultConnTestRunner
	pgxtest.RunValueRoundTripTests(context.Background(), t, ctrWithoutCodec, pgxtest.AllQueryExecModes, "hstore", tests)

	// scan empty and NULL: should be different in all query modes
	pgxtest.RunWithQueryExecModes(context.Background(), t, ctr, pgxtest.AllQueryExecModes, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		h := pgtype.Hstore{"should_be_erased": nil}
		err := conn.QueryRow(ctx, `select cast(null as hstore)`).Scan(&h)
		if err != nil {
			t.Fatal(err)
		}
		expectedNil := pgtype.Hstore(nil)
		if !reflect.DeepEqual(h, expectedNil) {
			t.Errorf("plain conn.Scan failed expectedNil=%#v actual=%#v", expectedNil, h)
		}

		err = conn.QueryRow(ctx, `select cast('' as hstore)`).Scan(&h)
		if err != nil {
			t.Fatal(err)
		}
		expectedEmpty := pgtype.Hstore{}
		if !reflect.DeepEqual(h, expectedEmpty) {
			t.Errorf("plain conn.Scan failed expectedEmpty=%#v actual=%#v", expectedEmpty, h)
		}
	})
}

func TestParseInvalidInputs(t *testing.T) {
	// these inputs should be invalid, but previously were considered correct
	invalidInputs := []string{
		`"a"=>"1", ,b"=>"2"`,
		`""=>"", 0"=>""`,
	}
	for i, input := range invalidInputs {
		var hstore pgtype.Hstore
		err := hstore.Scan(input)
		if err == nil {
			t.Errorf("test %d: input=%s (%#v) should fail; parsed correctly", i, input, input)
		}
	}
}
