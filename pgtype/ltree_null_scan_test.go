package pgtype_test

import (
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
)

// ltree is an extension type, so it is not in the default map and has to be
// registered by the user before it can be scanned.
const ltreeTestOID = 999904

func ltreeTestMap() *pgtype.Map {
	m := pgtype.NewMap()
	m.RegisterType(&pgtype.Type{Name: "ltree", OID: ltreeTestOID, Codec: pgtype.LtreeCodec{}})
	return m
}

// A NULL column reaches a scan plan as a nil src. Every binary scan plan in
// this package checks for that and reports a NULL scan, and the text format
// path for ltree does too, because it delegates to TextCodec. The two binary
// scan plans read src[0] to get the version byte before checking anything, so
// scanning a NULL ltree column into a *string or a *pgtype.Text panics instead
// of reporting the NULL. rows.Scan calls the plan with a nil src for a NULL
// field, so this is reachable from ordinary query scanning.
func TestLtreeCodecBinaryScanNull(t *testing.T) {
	m := ltreeTestMap()

	t.Run("string", func(t *testing.T) {
		dst := new(string)
		plan := m.PlanScan(ltreeTestOID, pgtype.BinaryFormatCode, dst)
		if plan == nil {
			t.Fatal("no scan plan for *string")
		}

		err := plan.Scan(nil, dst)
		if err == nil {
			t.Fatalf("expected an error scanning NULL into *string, got nil (dst is now %q)", *dst)
		}
		if *dst != "" {
			t.Errorf("destination was modified: %q", *dst)
		}
	})

	t.Run("text", func(t *testing.T) {
		dst := new(pgtype.Text)
		plan := m.PlanScan(ltreeTestOID, pgtype.BinaryFormatCode, dst)
		if plan == nil {
			t.Fatal("no scan plan for *pgtype.Text")
		}

		if err := plan.Scan(nil, dst); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if dst.Valid {
			t.Errorf("expected an invalid Text, got %v", *dst)
		}
	})
}

// The round trip in ltree_test.go needs a server, so check the non-NULL binary
// path here as well to make sure the NULL guard added above did not disturb
// ordinary decoding.
func TestLtreeCodecBinaryScanValue(t *testing.T) {
	m := ltreeTestMap()

	// Version byte 1 followed by the label.
	src := append([]byte{1}, "Top.Science.Astronomy"...)

	t.Run("string", func(t *testing.T) {
		dst := new(string)
		plan := m.PlanScan(ltreeTestOID, pgtype.BinaryFormatCode, dst)
		if plan == nil {
			t.Fatal("no scan plan for *string")
		}

		if err := plan.Scan(src, dst); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if *dst != "Top.Science.Astronomy" {
			t.Errorf("got %q, want %q", *dst, "Top.Science.Astronomy")
		}
	})

	t.Run("text", func(t *testing.T) {
		dst := new(pgtype.Text)
		plan := m.PlanScan(ltreeTestOID, pgtype.BinaryFormatCode, dst)
		if plan == nil {
			t.Fatal("no scan plan for *pgtype.Text")
		}

		if err := plan.Scan(src, dst); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if want := (pgtype.Text{String: "Top.Science.Astronomy", Valid: true}); *dst != want {
			t.Errorf("got %v, want %v", *dst, want)
		}
	})
}
