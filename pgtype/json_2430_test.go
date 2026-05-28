package pgtype_test

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
)

// TestJSONCodecDecodeValueJSONNullLiteral guards against regression of #2430:
// the JSON document `null` is a valid non-NULL value distinct from SQL NULL.
// JSONCodec.DecodeValue and JSONBCodec.DecodeValue must surface that distinction
// to callers like rows.Values() (which uses DecodeValue), instead of collapsing
// both to Go nil.
func TestJSONCodecDecodeValueJSONNullLiteral(t *testing.T) {
	t.Parallel()

	jsonCodec := &pgtype.JSONCodec{Marshal: json.Marshal, Unmarshal: json.Unmarshal}
	jsonbCodec := &pgtype.JSONBCodec{Marshal: json.Marshal, Unmarshal: json.Unmarshal}

	cases := []struct {
		name   string
		decode func([]byte) (any, error)
		src    []byte
		want   any
	}{
		// SQL NULL — src nil — must continue to return Go nil.
		{"json/sql_null", func(b []byte) (any, error) {
			return jsonCodec.DecodeValue(nil, 0, pgtype.TextFormatCode, b)
		}, nil, nil},
		{"jsonb/sql_null/text", func(b []byte) (any, error) {
			return jsonbCodec.DecodeValue(nil, 0, pgtype.TextFormatCode, b)
		}, nil, nil},

		// JSON null literal — src non-nil — must return raw bytes (not Go nil).
		{"json/json_null_literal", func(b []byte) (any, error) {
			return jsonCodec.DecodeValue(nil, 0, pgtype.TextFormatCode, b)
		}, []byte("null"), []byte("null")},
		{"jsonb/json_null_literal/text", func(b []byte) (any, error) {
			return jsonbCodec.DecodeValue(nil, 0, pgtype.TextFormatCode, b)
		}, []byte("null"), []byte("null")},
		{"jsonb/json_null_literal/binary", func(b []byte) (any, error) {
			return jsonbCodec.DecodeValue(nil, 0, pgtype.BinaryFormatCode, b)
		}, []byte{1, 'n', 'u', 'l', 'l'}, []byte("null")},

		// Existing non-null JSON values must still unmarshal as before.
		{"json/string", func(b []byte) (any, error) {
			return jsonCodec.DecodeValue(nil, 0, pgtype.TextFormatCode, b)
		}, []byte(`"hello"`), "hello"},
		{"jsonb/number/text", func(b []byte) (any, error) {
			return jsonbCodec.DecodeValue(nil, 0, pgtype.TextFormatCode, b)
		}, []byte("42"), float64(42)},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := tc.decode(tc.src)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			switch want := tc.want.(type) {
			case nil:
				if got != nil {
					t.Fatalf("want nil, got %v (%T)", got, got)
				}
			case []byte:
				gotBytes, ok := got.([]byte)
				if !ok {
					t.Fatalf("want []byte, got %T (%v)", got, got)
				}
				if !bytes.Equal(gotBytes, want) {
					t.Fatalf("want %q, got %q", want, gotBytes)
				}
			default:
				if got != tc.want {
					t.Fatalf("want %v (%T), got %v (%T)", tc.want, tc.want, got, got)
				}
			}
		})
	}
}
