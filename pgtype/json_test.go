package pgtype_test

import (
	"context"
	"testing"

	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxtest"
	"github.com/stretchr/testify/require"
)

func isExpectedEqMap(a any) func(any) bool {
	return func(v any) bool {
		aa := a.(map[string]any)
		bb := v.(map[string]any)

		if (aa == nil) != (bb == nil) {
			return false
		}

		if aa == nil {
			return true
		}

		if len(aa) != len(bb) {
			return false
		}

		for k := range aa {
			if aa[k] != bb[k] {
				return false
			}
		}

		return true
	}
}

func TestJSONCodec(t *testing.T) {
	type jsonStruct struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}

	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "json", []pgxtest.ValueRoundTripTest{
		{nil, new(*jsonStruct), isExpectedEq((*jsonStruct)(nil))},
		{map[string]any(nil), new(*string), isExpectedEq((*string)(nil))},
		{map[string]any(nil), new([]byte), isExpectedEqBytes([]byte(nil))},
		{[]byte(nil), new([]byte), isExpectedEqBytes([]byte(nil))},
		{nil, new([]byte), isExpectedEqBytes([]byte(nil))},
	})

	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, pgxtest.KnownOIDQueryExecModes, "json", []pgxtest.ValueRoundTripTest{
		{[]byte("{}"), new([]byte), isExpectedEqBytes([]byte("{}"))},
		{[]byte("null"), new([]byte), isExpectedEqBytes([]byte("null"))},
		{[]byte("42"), new([]byte), isExpectedEqBytes([]byte("42"))},
		{[]byte(`"hello"`), new([]byte), isExpectedEqBytes([]byte(`"hello"`))},
		{[]byte(`"hello"`), new(string), isExpectedEq(`"hello"`)},
		{map[string]any{"foo": "bar"}, new(map[string]any), isExpectedEqMap(map[string]any{"foo": "bar"})},
		{jsonStruct{Name: "Adam", Age: 10}, new(jsonStruct), isExpectedEq(jsonStruct{Name: "Adam", Age: 10})},
	})
}

// https://github.com/jackc/pgx/issues/1273#issuecomment-1221414648
func TestJSONCodecUnmarshalSQLNull(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		// Slices are nilified
		slice := []string{"foo", "bar", "baz"}
		err := conn.QueryRow(ctx, "select null::json").Scan(&slice)
		require.NoError(t, err)
		require.Nil(t, slice)

		// Maps are nilified
		m := map[string]any{"foo": "bar"}
		err = conn.QueryRow(ctx, "select null::json").Scan(&m)
		require.NoError(t, err)
		require.Nil(t, m)

		// Pointer to pointer are nilified
		n := 42
		p := &n
		err = conn.QueryRow(ctx, "select null::json").Scan(&p)
		require.NoError(t, err)
		require.Nil(t, p)

		// A string cannot scan a NULL.
		str := "foobar"
		err = conn.QueryRow(ctx, "select null::json").Scan(&str)
		require.EqualError(t, err, "can't scan into dest[0]: cannot scan NULL into *string")

		// A non-string cannot scan a NULL.
		err = conn.QueryRow(ctx, "select null::json").Scan(&n)
		require.EqualError(t, err, "can't scan into dest[0]: cannot scan NULL into *int")
	})
}

func TestJSONCodecClearExistingValueBeforeUnmarshal(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		m := map[string]any{}
		err := conn.QueryRow(ctx, `select '{"foo": "bar"}'::json`).Scan(&m)
		require.NoError(t, err)
		require.Equal(t, map[string]any{"foo": "bar"}, m)

		err = conn.QueryRow(ctx, `select '{"baz": "quz"}'::json`).Scan(&m)
		require.NoError(t, err)
		require.Equal(t, map[string]any{"baz": "quz"}, m)
	})
}
