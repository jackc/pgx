package pgtype_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"

	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
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

	var str string
	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "json", []pgxtest.ValueRoundTripTest{
		{nil, new(*jsonStruct), isExpectedEq((*jsonStruct)(nil))},
		{map[string]any(nil), new(*string), isExpectedEq((*string)(nil))},
		{map[string]any(nil), new([]byte), isExpectedEqBytes([]byte(nil))},
		{[]byte(nil), new([]byte), isExpectedEqBytes([]byte(nil))},
		{nil, new([]byte), isExpectedEqBytes([]byte(nil))},

		// Test sql.Scanner. (https://github.com/jackc/pgx/issues/1418)
		{"42", new(sql.NullInt64), isExpectedEq(sql.NullInt64{Int64: 42, Valid: true})},

		// Test driver.Valuer. (https://github.com/jackc/pgx/issues/1430)
		{sql.NullInt64{Int64: 42, Valid: true}, new(sql.NullInt64), isExpectedEq(sql.NullInt64{Int64: 42, Valid: true})},

		// Test driver.Valuer is used before json.Marshaler (https://github.com/jackc/pgx/issues/1805)
		{Issue1805(7), new(Issue1805), isExpectedEq(Issue1805(7))},
		// Test driver.Scanner is used before json.Unmarshaler (https://github.com/jackc/pgx/issues/2146)
		{Issue2146(7), new(*Issue2146), isPtrExpectedEq(Issue2146(7))},

		// Test driver.Scanner without pointer receiver (https://github.com/jackc/pgx/issues/2204)
		{NonPointerJSONScanner{V: stringPtr("{}")}, NonPointerJSONScanner{V: &str}, func(a any) bool { return str == "{}" }},
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

type Issue1805 int

func (i *Issue1805) Scan(src any) error {
	var source []byte
	switch src.(type) {
	case string:
		source = []byte(src.(string))
	case []byte:
		source = src.([]byte)
	default:
		return errors.New("unknown source type")
	}
	var newI int
	if err := json.Unmarshal(source, &newI); err != nil {
		return err
	}
	*i = Issue1805(newI)
	return nil
}

func (i Issue1805) Value() (driver.Value, error) {
	b, err := json.Marshal(int(i))
	return string(b), err
}

func (i Issue1805) UnmarshalJSON(bytes []byte) error {
	return errors.New("UnmarshalJSON called")
}

func (i Issue1805) MarshalJSON() ([]byte, error) {
	return nil, errors.New("MarshalJSON called")
}

type Issue2146 int

func (i *Issue2146) Scan(src any) error {
	var source []byte
	switch src.(type) {
	case string:
		source = []byte(src.(string))
	case []byte:
		source = src.([]byte)
	default:
		return errors.New("unknown source type")
	}
	var newI int
	if err := json.Unmarshal(source, &newI); err != nil {
		return err
	}
	*i = Issue2146(newI + 1)
	return nil
}

func (i Issue2146) Value() (driver.Value, error) {
	b, err := json.Marshal(int(i - 1))
	return string(b), err
}

type NonPointerJSONScanner struct {
	V *string
}

func (i NonPointerJSONScanner) Scan(src any) error {
	switch c := src.(type) {
	case string:
		*i.V = c
	case []byte:
		*i.V = string(c)
	default:
		return errors.New("unknown source type")
	}

	return nil
}

func (i NonPointerJSONScanner) Value() (driver.Value, error) {
	return i.V, nil
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

		m = map[string]interface{}{"foo": "bar"}
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
		fieldName := "json"
		if conn.PgConn().ParameterStatus("crdb_version") != "" {
			fieldName = "jsonb" // Seems like CockroachDB treats json as jsonb.
		}
		require.EqualError(t, err, fmt.Sprintf("can't scan into dest[0] (col: %s): cannot scan NULL into *string", fieldName))

		// A non-string cannot scan a NULL.
		err = conn.QueryRow(ctx, "select null::json").Scan(&n)
		require.EqualError(t, err, fmt.Sprintf("can't scan into dest[0] (col: %s): cannot scan NULL into *int", fieldName))
	})
}

// https://github.com/jackc/pgx/issues/1470
func TestJSONCodecPointerToPointerToString(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		var s *string
		err := conn.QueryRow(ctx, "select '{}'::json").Scan(&s)
		require.NoError(t, err)
		require.NotNil(t, s)
		require.Equal(t, "{}", *s)

		err = conn.QueryRow(ctx, "select null::json").Scan(&s)
		require.NoError(t, err)
		require.Nil(t, s)
	})
}

// https://github.com/jackc/pgx/issues/1691
func TestJSONCodecPointerToPointerToInt(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		n := 44
		p := &n
		err := conn.QueryRow(ctx, "select 'null'::jsonb").Scan(&p)
		require.NoError(t, err)
		require.Nil(t, p)
	})
}

// https://github.com/jackc/pgx/issues/1691
func TestJSONCodecPointerToPointerToStruct(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		type ImageSize struct {
			Height int    `json:"height"`
			Width  int    `json:"width"`
			Str    string `json:"str"`
		}
		is := &ImageSize{Height: 100, Width: 100, Str: "str"}
		err := conn.QueryRow(ctx, `select 'null'::jsonb`).Scan(&is)
		require.NoError(t, err)
		require.Nil(t, is)
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

type ParentIssue1681 struct {
	Child ChildIssue1681
}

func (t *ParentIssue1681) MarshalJSON() ([]byte, error) {
	return []byte(`{"custom":"thing"}`), nil
}

type ChildIssue1681 struct{}

func (t ChildIssue1681) MarshalJSON() ([]byte, error) {
	return []byte(`{"someVal": false}`), nil
}

// https://github.com/jackc/pgx/issues/1681
func TestJSONCodecEncodeJSONMarshalerThatCanBeWrapped(t *testing.T) {
	skipCockroachDB(t, "CockroachDB treats json as jsonb. This causes it to format differently than PostgreSQL.")

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		var jsonStr string
		err := conn.QueryRow(context.Background(), "select $1::json", &ParentIssue1681{}).Scan(&jsonStr)
		require.NoError(t, err)
		require.Equal(t, `{"custom":"thing"}`, jsonStr)
	})
}

func TestJSONCodecCustomMarshal(t *testing.T) {
	skipCockroachDB(t, "CockroachDB treats json as jsonb. This causes it to format differently than PostgreSQL.")

	connTestRunner := defaultConnTestRunner
	connTestRunner.AfterConnect = func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		conn.TypeMap().RegisterType(&pgtype.Type{
			Name: "json", OID: pgtype.JSONOID, Codec: &pgtype.JSONCodec{
				Marshal: func(v any) ([]byte, error) {
					return []byte(`{"custom":"value"}`), nil
				},
				Unmarshal: func(data []byte, v any) error {
					return json.Unmarshal([]byte(`{"custom":"value"}`), v)
				},
			},
		})
	}

	pgxtest.RunValueRoundTripTests(context.Background(), t, connTestRunner, pgxtest.KnownOIDQueryExecModes, "json", []pgxtest.ValueRoundTripTest{
		// There is no space between "custom" and "value" in json type.
		{map[string]any{"something": "else"}, new(string), isExpectedEq(`{"custom":"value"}`)},
		{[]byte(`{"something":"else"}`), new(map[string]any), func(v any) bool {
			return reflect.DeepEqual(v, map[string]any{"custom": "value"})
		}},
	})
}

func TestJSONCodecScanToNonPointerValues(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		n := 44
		err := conn.QueryRow(ctx, "select '42'::jsonb").Scan(n)
		require.Error(t, err)

		var i *int
		err = conn.QueryRow(ctx, "select '42'::jsonb").Scan(i)
		require.Error(t, err)

		m := 0
		err = conn.QueryRow(ctx, "select '42'::jsonb").Scan(&m)
		require.NoError(t, err)
		require.Equal(t, 42, m)
	})
}

func TestJSONCodecScanNull(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		var dest struct{}
		err := conn.QueryRow(ctx, "select null::jsonb").Scan(&dest)
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot scan NULL into *struct {}")

		err = conn.QueryRow(ctx, "select 'null'::jsonb").Scan(&dest)
		require.NoError(t, err)

		var destPointer *struct{}
		err = conn.QueryRow(ctx, "select null::jsonb").Scan(&destPointer)
		require.NoError(t, err)
		require.Nil(t, destPointer)

		err = conn.QueryRow(ctx, "select 'null'::jsonb").Scan(&destPointer)
		require.NoError(t, err)
		require.Nil(t, destPointer)

		var raw json.RawMessage
		require.NoError(t, conn.QueryRow(ctx, "select 'null'::jsonb").Scan(&raw))
		require.Equal(t, json.RawMessage("null"), raw)
	})
}

func TestJSONCodecScanNullToPointerToSQLScanner(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		var dest *Issue2146
		err := conn.QueryRow(ctx, "select null::jsonb").Scan(&dest)
		require.NoError(t, err)
		require.Nil(t, dest)
	})
}
