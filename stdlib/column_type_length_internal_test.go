package stdlib

import (
	"math"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// testRows is a pgx.Rows that only reports field descriptions, which is all
// ColumnTypeLength reads.
type testRows struct{ fd []pgconn.FieldDescription }

func (r testRows) Close()                                       {}
func (r testRows) Err() error                                   { return nil }
func (r testRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r testRows) FieldDescriptions() []pgconn.FieldDescription { return r.fd }
func (r testRows) Next() bool                                   { return false }
func (r testRows) Scan(dest ...any) error                       { return nil }
func (r testRows) Values() ([]any, error)                       { return nil, nil }
func (r testRows) RawValues() [][]byte                          { return nil }
func (r testRows) Conn() *pgx.Conn                              { return nil }
func (r testRows) TypeMap() *pgtype.Map                         { return nil }

func TestRowsColumnTypeLengthUnbounded(t *testing.T) {
	for _, tt := range []struct {
		name    string
		oid     uint32
		typmod  int32
		wantLen int64
		wantOK  bool
	}{
		{"unconstrained varchar", pgtype.VarcharOID, -1, math.MaxInt64, true},
		{"unconstrained bpchar", pgtype.BPCharOID, -1, math.MaxInt64, true},
		{"unconstrained varbit", pgtype.VarbitOID, -1, math.MaxInt64, true},
		{"varchar(255)", pgtype.VarcharOID, 259, 255, true},
		{"bpchar(10)", pgtype.BPCharOID, 14, 10, true},
		{"varbit(50)", pgtype.VarbitOID, 50, 50, true},
		{"text", pgtype.TextOID, -1, math.MaxInt64, true},
		{"int4", pgtype.Int4OID, -1, 0, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := &Rows{rows: testRows{fd: []pgconn.FieldDescription{{DataTypeOID: tt.oid, TypeModifier: tt.typmod}}}}
			gotLen, gotOK := r.ColumnTypeLength(0)
			if gotLen != tt.wantLen || gotOK != tt.wantOK {
				t.Fatalf("ColumnTypeLength() = (%d, %v), want (%d, %v)", gotLen, gotOK, tt.wantLen, tt.wantOK)
			}
		})
	}
}
