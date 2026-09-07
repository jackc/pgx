package pgtype_test

import (
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
)

// int4Field builds the binary wire encoding of one non-NULL int4 record field.
func int4Field(v byte) []byte {
	return []byte{0x00, 0x00, 0x00, 0x17, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, v}
}

// A binary record header carries a signed int32 field count taken straight from
// the wire. A negative count reached make([]any, count) and panicked with
// "makeslice: len out of range", and an oversized count requested an allocation
// unrelated to the amount of data actually present.
func TestRecordBinaryDecodeMalformedFieldCountReturnsError(t *testing.T) {
	m := pgtype.NewMap()
	dt, ok := m.TypeForOID(pgtype.RecordOID)
	if !ok {
		t.Fatal("no type registered for record OID")
	}

	for _, tt := range []struct {
		name string
		src  []byte
	}{
		{"negative", []byte{0xff, 0xff, 0xff, 0xff}},
		{"negative min", []byte{0x80, 0x00, 0x00, 0x00}},
		{"oversized", []byte{0x7f, 0xff, 0xff, 0xff}},
		{"count exceeds data", []byte{0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x17, 0xff, 0xff, 0xff, 0xff}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			// A panic here (pre-fix) fails the test.
			if _, err := dt.Codec.DecodeValue(m, pgtype.RecordOID, pgtype.BinaryFormatCode, tt.src); err == nil {
				t.Errorf("DecodeValue(% x) = nil error, want an error", tt.src)
			}
		})
	}
}

// A field count that understates how many fields follow must not run past the
// end of the presized value slice.
func TestRecordBinaryDecodeFieldCountUnderstated(t *testing.T) {
	m := pgtype.NewMap()
	dt, ok := m.TypeForOID(pgtype.RecordOID)
	if !ok {
		t.Fatal("no type registered for record OID")
	}

	for _, tt := range []struct {
		name string
		src  []byte
		want int
	}{
		{"count 0, one field", append([]byte{0x00, 0x00, 0x00, 0x00}, int4Field(1)...), 1},
		{"count 1, two fields", append(append([]byte{0x00, 0x00, 0x00, 0x01}, int4Field(1)...), int4Field(2)...), 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			// A panic here (pre-fix) fails the test.
			v, err := dt.Codec.DecodeValue(m, pgtype.RecordOID, pgtype.BinaryFormatCode, tt.src)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			values, ok := v.([]any)
			if !ok {
				t.Fatalf("got %T, want []any", v)
			}
			if len(values) != tt.want {
				t.Errorf("got %d values (%v), want %d", len(values), values, tt.want)
			}
		})
	}
}

// Scanning a row with more fields than the destination CompositeFields has
// slots indexed past the end of the slice and panicked. This needs no malformed
// input — "select row('foo'::text, 42::int4)" into a one-element
// CompositeFields is enough.
func TestRecordScanUndersizedCompositeFieldsReturnsError(t *testing.T) {
	m := pgtype.NewMap()

	src := append(append([]byte{0x00, 0x00, 0x00, 0x02}, int4Field(1)...), int4Field(2)...)

	var a int32
	dst := pgtype.CompositeFields{&a}

	plan := m.PlanScan(pgtype.RecordOID, pgtype.BinaryFormatCode, dst)
	// A panic here (pre-fix) fails the test.
	if err := plan.Scan(src, dst); err == nil {
		t.Error("Scan into undersized CompositeFields = nil error, want an error")
	}
}

// The composite codec reaches the same destination indexing through its own
// scan plan.
func TestCompositeScanUndersizedCompositeFieldsReturnsError(t *testing.T) {
	m := pgtype.NewMap()
	int4Type, ok := m.TypeForOID(pgtype.Int4OID)
	if !ok {
		t.Fatal("no type registered for int4 OID")
	}

	const compositeOID = 100000
	m.RegisterType(&pgtype.Type{
		Name: "test_composite",
		OID:  compositeOID,
		Codec: &pgtype.CompositeCodec{Fields: []pgtype.CompositeCodecField{
			{Name: "a", Type: int4Type},
			{Name: "b", Type: int4Type},
		}},
	})

	src := append(append([]byte{0x00, 0x00, 0x00, 0x02}, int4Field(1)...), int4Field(2)...)

	var a int32
	dst := pgtype.CompositeFields{&a}

	plan := m.PlanScan(compositeOID, pgtype.BinaryFormatCode, dst)
	// A panic here (pre-fix) fails the test.
	if err := plan.Scan(src, dst); err == nil {
		t.Error("Scan into undersized CompositeFields = nil error, want an error")
	}
}

// Well-formed records still decode after the guards were added.
func TestRecordBinaryDecodeValid(t *testing.T) {
	m := pgtype.NewMap()
	dt, ok := m.TypeForOID(pgtype.RecordOID)
	if !ok {
		t.Fatal("no type registered for record OID")
	}

	for _, tt := range []struct {
		name string
		src  []byte
		want []any
	}{
		{"empty", []byte{0x00, 0x00, 0x00, 0x00}, []any{}},
		{"one field", append([]byte{0x00, 0x00, 0x00, 0x01}, int4Field(1)...), []any{int32(1)}},
		{
			"two fields",
			append(append([]byte{0x00, 0x00, 0x00, 0x02}, int4Field(1)...), int4Field(2)...),
			[]any{int32(1), int32(2)},
		},
		{
			"null field",
			[]byte{0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x17, 0xff, 0xff, 0xff, 0xff},
			[]any{nil},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			v, err := dt.Codec.DecodeValue(m, pgtype.RecordOID, pgtype.BinaryFormatCode, tt.src)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			values, ok := v.([]any)
			if !ok {
				t.Fatalf("got %T, want []any", v)
			}
			if len(values) != len(tt.want) {
				t.Fatalf("got %v, want %v", values, tt.want)
			}
			for i := range tt.want {
				if values[i] != tt.want[i] {
					t.Errorf("field %d: got %v, want %v", i, values[i], tt.want[i])
				}
			}
		})
	}
}
