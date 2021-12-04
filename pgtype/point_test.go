package pgtype_test

import (
	"reflect"
	"testing"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgtype/testutil"
	"github.com/stretchr/testify/require"
)

func TestPointTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscode(t, "point", []interface{}{
		&pgtype.Point{P: pgtype.Vec2{1.234, 5.6789012345}, Valid: true},
		&pgtype.Point{P: pgtype.Vec2{-1.234, -5.6789}, Valid: true},
		&pgtype.Point{},
	})
}

func TestPoint_Set(t *testing.T) {
	tests := []struct {
		name    string
		arg     interface{}
		valid   bool
		wantErr bool
	}{
		{
			name:    "first",
			arg:     "(12312.123123,123123.123123)",
			valid:   true,
			wantErr: false,
		},
		{
			name:    "second",
			arg:     "(1231s2.123123,123123.123123)",
			valid:   false,
			wantErr: true,
		},
		{
			name:    "third",
			arg:     []byte("(122.123123,123.123123)"),
			valid:   true,
			wantErr: false,
		},
		{
			name:    "third",
			arg:     nil,
			valid:   false,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dst := &pgtype.Point{}
			if err := dst.Set(tt.arg); (err != nil) != tt.wantErr {
				t.Errorf("Set() error = %v, wantErr %v", err, tt.wantErr)
			}
			if dst.Valid != tt.valid {
				t.Errorf("Expected status: %v; got: %v", tt.valid, dst.Valid)
			}
		})
	}
}

func TestPoint_MarshalJSON(t *testing.T) {
	tests := []struct {
		name  string
		point pgtype.Point
		want  []byte
	}{
		{
			name: "second",
			point: pgtype.Point{
				P:     pgtype.Vec2{X: 12.245, Y: 432.12},
				Valid: true,
			},
			want: []byte(`"(12.245,432.12)"`),
		},
		{
			name: "third",
			point: pgtype.Point{
				P: pgtype.Vec2{},
			},
			want: []byte("null"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.point.MarshalJSON()
			require.NoError(t, err)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("MarshalJSON() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPoint_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name    string
		valid   bool
		arg     []byte
		wantErr bool
	}{
		{
			name:    "first",
			valid:   true,
			arg:     []byte(`"(123.123,54.12)"`),
			wantErr: false,
		},
		{
			name:    "second",
			valid:   false,
			arg:     []byte(`"(123.123,54.1sad2)"`),
			wantErr: true,
		},
		{
			name:    "third",
			valid:   false,
			arg:     []byte("null"),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dst := &pgtype.Point{}
			if err := dst.UnmarshalJSON(tt.arg); (err != nil) != tt.wantErr {
				t.Errorf("UnmarshalJSON() error = %v, wantErr %v", err, tt.wantErr)
			}
			if dst.Valid != tt.valid {
				t.Errorf("Valid mismatch: %v != %v", dst.Valid, tt.valid)
			}
		})
	}
}
