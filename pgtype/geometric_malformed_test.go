package pgtype_test

import (
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
)

// Malformed geometric text values that are missing an expected separator must
// return an error instead of panicking with "slice bounds out of range". The
// text scan plans in circle.go, box.go, lseg.go, path.go and polygon.go used
// the result of strings.IndexByte to slice without checking for -1.
func TestGeometricScanMalformedReturnsError(t *testing.T) {
	tests := []struct {
		name string
		scan func(string) error
		src  string
	}{
		{"Circle", func(s string) error { var v pgtype.Circle; return v.Scan(s) }, "<(123456789>"},
		{"Box", func(s string) error { var v pgtype.Box; return v.Scan(s) }, "(1234567890"},
		{"Lseg", func(s string) error { var v pgtype.Lseg; return v.Scan(s) }, "[(123456789"},
		{"Path", func(s string) error { var v pgtype.Path; return v.Scan(s) }, "((1234567"},
		{"Polygon", func(s string) error { var v pgtype.Polygon; return v.Scan(s) }, "((1234567"},
		// Inputs where the missing separator slips past an IndexByte == -1
		// guard but the subsequent fixed-offset slice (str[end+2:],
		// str[end+3:], str[:len-1]) still ran off the end. These panicked
		// even with the -1 guards in place and are covered by the strings.Cut
		// rewrite.
		{"CircleRadius", func(s string) error { var v pgtype.Circle; return v.Scan(s) }, "<(1,000000)"},
		{"BoxTail", func(s string) error { var v pgtype.Box; return v.Scan(s) }, "(0,000000000)"},
		{"LsegTail", func(s string) error { var v pgtype.Lseg; return v.Scan(s) }, "[(0,000000000)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// A panic here (pre-fix) fails the test.
			if err := tt.scan(tt.src); err == nil {
				t.Errorf("Scan(%q) = nil error, want an error", tt.src)
			}
		})
	}
}

// Well-formed geometric text values still parse correctly after the guards
// were added.
func TestGeometricScanValid(t *testing.T) {
	t.Run("Circle", func(t *testing.T) {
		var v pgtype.Circle
		if err := v.Scan("<(1.5,2.5),3.5>"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !v.Valid || v.P.X != 1.5 || v.P.Y != 2.5 || v.R != 3.5 {
			t.Errorf("got %+v", v)
		}
	})
	t.Run("Box", func(t *testing.T) {
		var v pgtype.Box
		if err := v.Scan("(3,4),(1,2)"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !v.Valid {
			t.Errorf("got %+v", v)
		}
	})
	t.Run("Lseg", func(t *testing.T) {
		var v pgtype.Lseg
		if err := v.Scan("[(1,2),(3,4)]"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !v.Valid {
			t.Errorf("got %+v", v)
		}
	})
	t.Run("Path", func(t *testing.T) {
		var v pgtype.Path
		if err := v.Scan("((1,2),(3,4))"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !v.Valid || len(v.P) != 2 {
			t.Errorf("got %+v", v)
		}
	})
	t.Run("Polygon", func(t *testing.T) {
		var v pgtype.Polygon
		if err := v.Scan("((1,2),(3,4))"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !v.Valid || len(v.P) != 2 {
			t.Errorf("got %+v", v)
		}
	})
}
