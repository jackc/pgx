package pgtype_test

import (
	"reflect"
	"testing"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgtype/testutil"
)

func TestPointTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscode(t, "point", []interface{}{
		&pgtype.Point{P: pgtype.Vec2{1.234, 5.6789012345}, Status: pgtype.Present},
		&pgtype.Point{P: pgtype.Vec2{-1.234, -5.6789}, Status: pgtype.Present},
		&pgtype.Point{Status: pgtype.Null},
	})
}

func TestPoint_Set(t *testing.T) {
	tests := []struct {
		name    string
		arg     interface{}
		status  pgtype.Status
		wantErr bool
	}{
		{
			name:    "first",
			arg:     "(12312.123123,123123.123123)",
			status:  pgtype.Present,
			wantErr: false,
		},
		{
			name:    "second",
			arg:     "(1231s2.123123,123123.123123)",
			status:  pgtype.Undefined,
			wantErr: true,
		},
		{
			name:    "third",
			arg:     []byte("(122.123123,123.123123)"),
			status:  pgtype.Present,
			wantErr: false,
		},
		{
			name:    "third",
			arg:     nil,
			status:  pgtype.Null,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dst := &pgtype.Point{}
			if err := dst.Set(tt.arg); (err != nil) != tt.wantErr {
				t.Errorf("Set() error = %v, wantErr %v", err, tt.wantErr)
			}
			if dst.Status != tt.status {
				t.Errorf("Expected status: %v; got: %v", tt.status, dst.Status)
			}
		})
	}
}

func TestPoint_MarshalJSON(t *testing.T) {
	tests := []struct {
		name    string
		point   pgtype.Point
		want    []byte
		wantErr bool
	}{
		{
			name: "first",
			point: pgtype.Point{
				P:      pgtype.Vec2{},
				Status: pgtype.Undefined,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "second",
			point: pgtype.Point{
				P:      pgtype.Vec2{X: 12.245, Y: 432.12},
				Status: pgtype.Present,
			},
			want:    []byte(`"(12.245,432.12)"`),
			wantErr: false,
		},
		{
			name: "third",
			point: pgtype.Point{
				P:      pgtype.Vec2{},
				Status: pgtype.Null,
			},
			want:    []byte("null"),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.point.MarshalJSON()
			if (err != nil) != tt.wantErr {
				t.Errorf("MarshalJSON() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("MarshalJSON() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPoint_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name    string
		status  pgtype.Status
		arg     []byte
		wantErr bool
	}{
		{
			name:    "first",
			status:  pgtype.Present,
			arg:     []byte(`"(123.123,54.12)"`),
			wantErr: false,
		},
		{
			name:    "second",
			status:  pgtype.Undefined,
			arg:     []byte(`"(123.123,54.1sad2)"`),
			wantErr: true,
		},
		{
			name:    "third",
			status:  pgtype.Null,
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
			if dst.Status != tt.status {
				t.Errorf("Status mismatch: %v != %v", dst.Status, tt.status)
			}
		})
	}
}
