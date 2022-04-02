package pgtype_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxtest"
	"github.com/stretchr/testify/require"
)

func TestPointCodec(t *testing.T) {
	skipCockroachDB(t, "Server does not support type point")

	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "point", []pgxtest.ValueRoundTripTest{
		{
			pgtype.Point{P: pgtype.Vec2{1.234, 5.6789012345}, Valid: true},
			new(pgtype.Point),
			isExpectedEq(pgtype.Point{P: pgtype.Vec2{1.234, 5.6789012345}, Valid: true}),
		},
		{
			pgtype.Point{P: pgtype.Vec2{-1.234, -5.6789}, Valid: true},
			new(pgtype.Point),
			isExpectedEq(pgtype.Point{P: pgtype.Vec2{-1.234, -5.6789}, Valid: true}),
		},
		{pgtype.Point{}, new(pgtype.Point), isExpectedEq(pgtype.Point{})},
		{nil, new(pgtype.Point), isExpectedEq(pgtype.Point{})},
	})
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
