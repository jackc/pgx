package pgtype_test

import (
	"testing"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgtype/testutil"
)

func TestPolygonTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscode(t, "polygon", []interface{}{
		&pgtype.Polygon{
			P:      []pgtype.Vec2{{3.14, 1.678901234}, {7.1, 5.234}, {5.0, 3.234}},
			Status: pgtype.Present,
		},
		&pgtype.Polygon{
			P:      []pgtype.Vec2{{3.14, -1.678}, {7.1, -5.234}, {23.1, 9.34}},
			Status: pgtype.Present,
		},
		&pgtype.Polygon{Status: pgtype.Null},
	})
}

func TestPolygon_Set(t *testing.T) {
	tests := []struct {
		name    string
		arg     interface{}
		status  pgtype.Status
		wantErr bool
	}{
		{
			name:    "string",
			arg:     "((3.14,1.678901234),(7.1,5.234),(5.0,3.234))",
			status:  pgtype.Present,
			wantErr: false,
		}, {
			name:    "[]float64",
			arg:     []float64{1, 2, 3.45, 6.78, 1.23, 4.567, 8.9, 1.0},
			status:  pgtype.Present,
			wantErr: false,
		}, {
			name:    "[]Vec2",
			arg:     []pgtype.Vec2{{1, 2}, {2.3, 4.5}, {6.78, 9.123}},
			status:  pgtype.Present,
			wantErr: false,
		}, {
			name:    "null",
			arg:     nil,
			status:  pgtype.Null,
			wantErr: false,
		}, {
			name:    "invalid_string_1",
			arg:     "((3.14,1.678901234),(7.1,5.234),(5.0,3.234x))",
			status:  pgtype.Undefined,
			wantErr: true,
		}, {
			name:    "invalid_string_2",
			arg:     "(3,4)",
			status:  pgtype.Undefined,
			wantErr: true,
		}, {
			name:    "invalid_[]float64",
			arg:     []float64{1, 2, 3.45, 6.78, 1.23, 4.567, 8.9},
			status:  pgtype.Undefined,
			wantErr: true,
		}, {
			name:    "invalid_type",
			arg:     []int{1, 2, 3, 6},
			status:  pgtype.Undefined,
			wantErr: true,
		}, {
			name:    "empty_[]float64",
			arg:     []float64{},
			status:  pgtype.Null,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dst := &pgtype.Polygon{}
			if err := dst.Set(tt.arg); (err != nil) != tt.wantErr {
				t.Errorf("Set() error = %v, wantErr %v", err, tt.wantErr)
			}
			if dst.Status != tt.status {
				t.Errorf("Expected status: %v; got: %v", tt.status, dst.Status)
			}
		})
	}
}
