package pgtype_test

import (
	"context"
	"testing"

	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxtest"
	"github.com/stretchr/testify/require"
)

func TestJSONBTranscode(t *testing.T) {
	type jsonStruct struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}

	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "jsonb", []pgxtest.ValueRoundTripTest{
		{nil, new(*jsonStruct), isExpectedEq((*jsonStruct)(nil))},
		{map[string]any(nil), new(*string), isExpectedEq((*string)(nil))},
		{map[string]any(nil), new([]byte), isExpectedEqBytes([]byte(nil))},
		{[]byte(nil), new([]byte), isExpectedEqBytes([]byte(nil))},
		{nil, new([]byte), isExpectedEqBytes([]byte(nil))},
	})

	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, pgxtest.KnownOIDQueryExecModes, "jsonb", []pgxtest.ValueRoundTripTest{
		{[]byte("{}"), new([]byte), isExpectedEqBytes([]byte("{}"))},
		{[]byte("null"), new([]byte), isExpectedEqBytes([]byte("null"))},
		{[]byte("42"), new([]byte), isExpectedEqBytes([]byte("42"))},
		{[]byte(`"hello"`), new([]byte), isExpectedEqBytes([]byte(`"hello"`))},
		{[]byte(`"hello"`), new(string), isExpectedEq(`"hello"`)},
		{map[string]any{"foo": "bar"}, new(map[string]any), isExpectedEqMap(map[string]any{"foo": "bar"})},
		{jsonStruct{Name: "Adam", Age: 10}, new(jsonStruct), isExpectedEq(jsonStruct{Name: "Adam", Age: 10})},
	})
}

func TestJSONBCodecUnmarshalSQLNull(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		// Slices are nilified
		slice := []string{"foo", "bar", "baz"}
		err := conn.QueryRow(ctx, "select null::jsonb").Scan(&slice)
		require.NoError(t, err)
		require.Nil(t, slice)

		// Maps are nilified
		m := map[string]any{"foo": "bar"}
		err = conn.QueryRow(ctx, "select null::jsonb").Scan(&m)
		require.NoError(t, err)
		require.Nil(t, m)

		m = map[string]interface{}{"foo": "bar"}
		err = conn.QueryRow(ctx, "select null::jsonb").Scan(&m)
		require.NoError(t, err)
		require.Nil(t, m)

		// Pointer to pointer are nilified
		n := 42
		p := &n
		err = conn.QueryRow(ctx, "select null::jsonb").Scan(&p)
		require.NoError(t, err)
		require.Nil(t, p)

		// A string cannot scan a NULL.
		str := "foobar"
		err = conn.QueryRow(ctx, "select null::jsonb").Scan(&str)
		require.EqualError(t, err, "can't scan into dest[0]: cannot scan NULL into *string")

		// A non-string cannot scan a NULL.
		err = conn.QueryRow(ctx, "select null::jsonb").Scan(&n)
		require.EqualError(t, err, "can't scan into dest[0]: cannot scan NULL into *int")
	})
}

// https://github.com/jackc/pgx/issues/1681
func TestJSONBCodecEncodeJSONMarshalerThatCanBeWrapped(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		var jsonStr string
		err := conn.QueryRow(context.Background(), "select $1::jsonb", &ParentIssue1681{}).Scan(&jsonStr)
		require.NoError(t, err)
		require.Equal(t, `{"custom": "thing"}`, jsonStr) // Note that unlike json, jsonb reformats the JSON string.
	})
}
