package pgtype_test

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxtest"
	"github.com/stretchr/testify/require"
)

func TestJSONBTranscode(t *testing.T) {
	type jsonStruct struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}

	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "jsonb", []pgxtest.ValueRoundTripTest{
		{Param: nil, Result: new(*jsonStruct), Test: isExpectedEq((*jsonStruct)(nil))},
		{Param: map[string]any(nil), Result: new(*string), Test: isExpectedEq((*string)(nil))},
		{Param: map[string]any(nil), Result: new([]byte), Test: isExpectedEqBytes([]byte(nil))},
		{Param: []byte(nil), Result: new([]byte), Test: isExpectedEqBytes([]byte(nil))},
		{Param: nil, Result: new([]byte), Test: isExpectedEqBytes([]byte(nil))},
	})

	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, pgxtest.KnownOIDQueryExecModes, "jsonb", []pgxtest.ValueRoundTripTest{
		{Param: []byte("{}"), Result: new([]byte), Test: isExpectedEqBytes([]byte("{}"))},
		{Param: []byte("null"), Result: new([]byte), Test: isExpectedEqBytes([]byte("null"))},
		{Param: []byte("42"), Result: new([]byte), Test: isExpectedEqBytes([]byte("42"))},
		{Param: []byte(`"hello"`), Result: new([]byte), Test: isExpectedEqBytes([]byte(`"hello"`))},
		{Param: []byte(`"hello"`), Result: new(string), Test: isExpectedEq(`"hello"`)},
		{Param: map[string]any{"foo": "bar"}, Result: new(map[string]any), Test: isExpectedEqMap(map[string]any{"foo": "bar"})},
		{Param: jsonStruct{Name: "Adam", Age: 10}, Result: new(jsonStruct), Test: isExpectedEq(jsonStruct{Name: "Adam", Age: 10})},
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

		m = map[string]any{"foo": "bar"}
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
		require.EqualError(t, err, "can't scan into dest[0] (col: jsonb): cannot scan NULL into *string")

		// A non-string cannot scan a NULL.
		err = conn.QueryRow(ctx, "select null::jsonb").Scan(&n)
		require.EqualError(t, err, "can't scan into dest[0] (col: jsonb): cannot scan NULL into *int")
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

func TestJSONBCodecCustomMarshal(t *testing.T) {
	connTestRunner := defaultConnTestRunner
	connTestRunner.AfterConnect = func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		conn.TypeMap().RegisterType(&pgtype.Type{
			Name: "jsonb", OID: pgtype.JSONBOID, Codec: &pgtype.JSONBCodec{
				Marshal: func(v any) ([]byte, error) {
					return []byte(`{"custom":"value"}`), nil
				},
				Unmarshal: func(data []byte, v any) error {
					return json.Unmarshal([]byte(`{"custom":"value"}`), v)
				},
			},
		})
	}

	pgxtest.RunValueRoundTripTests(context.Background(), t, connTestRunner, pgxtest.KnownOIDQueryExecModes, "jsonb", []pgxtest.ValueRoundTripTest{
		// There is space between "custom" and "value" in jsonb type.
		{Param: map[string]any{"something": "else"}, Result: new(string), Test: isExpectedEq(`{"custom": "value"}`)},
		{Param: []byte(`{"something":"else"}`), Result: new(map[string]any), Test: func(v any) bool {
			return reflect.DeepEqual(v, map[string]any{"custom": "value"})
		}},
	})
}
