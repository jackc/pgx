package pgtype_test

import (
	"context"
	"database/sql"
	"reflect"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxtest"
)

func TestLtreeCodec(t *testing.T) {
	skipCockroachDB(t, "Server does not support type ltree")

	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, pgxtest.KnownOIDQueryExecModes, "ltree", []pgxtest.ValueRoundTripTest{
		{
			Param:  "A.B.C",
			Result: new(string),
			Test:   isExpectedEq("A.B.C"),
		},
		{
			Param:  pgtype.Text{String: "", Valid: true},
			Result: new(pgtype.Text),
			Test:   isExpectedEq(pgtype.Text{String: "", Valid: true}),
		},
	})
}

func TestLtreeCodecBinaryDecodeValue(t *testing.T) {
	t.Parallel()

	const (
		ltreeOID      = 999904
		ltreeArrayOID = 999905
	)

	m := pgtype.NewMap()
	ltreeType := &pgtype.Type{Name: "ltree", OID: ltreeOID, Codec: pgtype.LtreeCodec{}}
	m.RegisterType(ltreeType)
	m.RegisterType(&pgtype.Type{Name: "_ltree", OID: ltreeArrayOID, Codec: &pgtype.ArrayCodec{ElementType: ltreeType}})

	codec := pgtype.LtreeCodec{}

	t.Run("binary value", func(t *testing.T) {
		src, err := m.Encode(ltreeOID, pgtype.BinaryFormatCode, "Top.Science.Astronomy", nil)
		if err != nil {
			t.Fatalf("encode failed: %v", err)
		}

		got, err := codec.DecodeValue(m, ltreeOID, pgtype.BinaryFormatCode, src)
		if err != nil {
			t.Fatalf("DecodeValue failed: %v", err)
		}
		if got != "Top.Science.Astronomy" {
			t.Fatalf("DecodeValue got %q, want %q", got, "Top.Science.Astronomy")
		}

		var dst any
		if err := m.Scan(ltreeOID, pgtype.BinaryFormatCode, src, &dst); err != nil {
			t.Fatalf("Scan(&any) failed: %v", err)
		}
		if dst != "Top.Science.Astronomy" {
			t.Fatalf("Scan(&any) got %q, want %q", dst, "Top.Science.Astronomy")
		}
	})

	t.Run("binary empty ltree", func(t *testing.T) {
		got, err := codec.DecodeValue(m, ltreeOID, pgtype.BinaryFormatCode, []byte{1})
		if err != nil {
			t.Fatalf("DecodeValue failed: %v", err)
		}
		if got != "" {
			t.Fatalf("DecodeValue got %q, want %q", got, "")
		}
	})

	t.Run("binary null", func(t *testing.T) {
		got, err := codec.DecodeValue(m, ltreeOID, pgtype.BinaryFormatCode, nil)
		if err != nil {
			t.Fatalf("DecodeValue failed: %v", err)
		}
		if got != nil {
			t.Fatalf("DecodeValue got %#v, want nil", got)
		}
	})

	t.Run("text value", func(t *testing.T) {
		got, err := codec.DecodeValue(m, ltreeOID, pgtype.TextFormatCode, []byte("Top.Science"))
		if err != nil {
			t.Fatalf("DecodeValue failed: %v", err)
		}
		if got != "Top.Science" {
			t.Fatalf("DecodeValue got %q, want %q", got, "Top.Science")
		}
	})

	t.Run("binary ltree array DecodeValue", func(t *testing.T) {
		if fc := m.FormatCodeForOID(ltreeArrayOID); fc != pgtype.BinaryFormatCode {
			t.Fatalf("FormatCodeForOID(_ltree) = %d, want BinaryFormatCode", fc)
		}

		arrSrc, err := m.Encode(ltreeArrayOID, pgtype.BinaryFormatCode, []string{"Top.Science", "Top.Hobbies"}, nil)
		if err != nil {
			t.Fatalf("encode array failed: %v", err)
		}

		var got any
		if err := m.Scan(ltreeArrayOID, pgtype.BinaryFormatCode, arrSrc, &got); err != nil {
			t.Fatalf("Scan(_ltree) failed: %v", err)
		}
		want := []any{"Top.Science", "Top.Hobbies"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("Scan(_ltree) got %#v, want %#v", got, want)
		}
	})

	t.Run("malformed binary payloads", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			src  []byte
		}{
			{name: "empty non-nil", src: []byte{}},
			{name: "unsupported version", src: []byte{2, 'A', '.', 'B'}},
		} {
			if got, err := codec.DecodeValue(m, ltreeOID, pgtype.BinaryFormatCode, tc.src); err == nil {
				t.Fatalf("%s: expected error, got value %q", tc.name, got)
			}
		}
	})

	t.Run("unknown format code", func(t *testing.T) {
		if got, err := codec.DecodeValue(m, ltreeOID, 2, []byte("A.B")); err == nil {
			t.Fatalf("expected error for unknown format code, got %#v", got)
		}
	})
}

func TestLtreeCodecBinaryDecodeDatabaseSQLValue(t *testing.T) {
	t.Parallel()

	m := ltreeTestMap()
	codec := pgtype.LtreeCodec{}
	src := append([]byte{1}, "Top.Science.Astronomy"...)

	t.Run("binary value", func(t *testing.T) {
		got, err := codec.DecodeDatabaseSQLValue(m, ltreeTestOID, pgtype.BinaryFormatCode, src)
		if err != nil {
			t.Fatalf("DecodeDatabaseSQLValue failed: %v", err)
		}
		if got != "Top.Science.Astronomy" {
			t.Fatalf("DecodeDatabaseSQLValue got %q, want %q", got, "Top.Science.Astronomy")
		}

		var ns sql.NullString
		if err := m.Scan(ltreeTestOID, pgtype.BinaryFormatCode, src, &ns); err != nil {
			t.Fatalf("Scan(*sql.NullString) failed: %v", err)
		}
		if want := (sql.NullString{String: "Top.Science.Astronomy", Valid: true}); ns != want {
			t.Fatalf("Scan(*sql.NullString) got %+v, want %+v", ns, want)
		}
	})

	t.Run("binary null", func(t *testing.T) {
		var ns sql.NullString
		if err := m.Scan(ltreeTestOID, pgtype.BinaryFormatCode, nil, &ns); err != nil {
			t.Fatalf("Scan(*sql.NullString) NULL failed: %v", err)
		}
		if ns.Valid {
			t.Fatalf("Scan(*sql.NullString) NULL got %+v, want invalid", ns)
		}
	})

	t.Run("text value", func(t *testing.T) {
		got, err := codec.DecodeDatabaseSQLValue(m, ltreeTestOID, pgtype.TextFormatCode, []byte("Top.Science"))
		if err != nil {
			t.Fatalf("DecodeDatabaseSQLValue text failed: %v", err)
		}
		if got != "Top.Science" {
			t.Fatalf("DecodeDatabaseSQLValue text got %q, want %q", got, "Top.Science")
		}
	})

	t.Run("malformed binary payloads", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			src  []byte
		}{
			{name: "empty non-nil", src: []byte{}},
			{name: "unsupported version", src: []byte{2, 'A', '.', 'B'}},
		} {
			if got, err := codec.DecodeDatabaseSQLValue(m, ltreeTestOID, pgtype.BinaryFormatCode, tc.src); err == nil {
				t.Fatalf("%s: expected error, got %#v", tc.name, got)
			}
		}
	})
}

func TestLtreeCodecBinaryScanMalformed(t *testing.T) {
	t.Parallel()

	m := ltreeTestMap()

	for _, tc := range []struct {
		name string
		src  []byte
	}{
		{name: "empty non-nil", src: []byte{}},
		{name: "unsupported version", src: []byte{2, 'A', '.', 'B'}},
	} {
		t.Run("string/"+tc.name, func(t *testing.T) {
			var dst string
			if err := m.Scan(ltreeTestOID, pgtype.BinaryFormatCode, tc.src, &dst); err == nil {
				t.Fatalf("expected error, got nil (dst=%q)", dst)
			}
		})

		t.Run("text/"+tc.name, func(t *testing.T) {
			var dst pgtype.Text
			if err := m.Scan(ltreeTestOID, pgtype.BinaryFormatCode, tc.src, &dst); err == nil {
				t.Fatalf("expected error, got nil (dst=%+v)", dst)
			}
		})
	}
}
