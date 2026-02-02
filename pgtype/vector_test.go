package pgtype_test

import (
	"encoding/binary"
	"math"
	"reflect"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
)

func TestVectorMarshalJSON(t *testing.T) {
	tests := []struct {
		name   string
		vector pgtype.Vector
		want   string
	}{
		{
			name:   "valid vector",
			vector: pgtype.Vector{Vec: []float32{1, 2, 3}, Valid: true},
			want:   "[1,2,3]",
		},
		{
			name:   "empty vector",
			vector: pgtype.Vector{Vec: []float32{}, Valid: true},
			want:   "[]",
		},
		{
			name:   "null vector",
			vector: pgtype.Vector{},
			want:   "null",
		},
		{
			name:   "vector with decimals",
			vector: pgtype.Vector{Vec: []float32{1.5, 2.25, 3.75}, Valid: true},
			want:   "[1.5,2.25,3.75]",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.vector.MarshalJSON()
			require.NoError(t, err)
			require.Equal(t, tt.want, string(got))
		})
	}
}

func TestVectorUnmarshalJSON(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    pgtype.Vector
		wantErr bool
	}{
		{
			name:    "valid vector",
			input:   "[1,2,3]",
			want:    pgtype.Vector{Vec: []float32{1, 2, 3}, Valid: true},
			wantErr: false,
		},
		{
			name:    "empty vector",
			input:   "[]",
			want:    pgtype.Vector{Vec: []float32{}, Valid: true},
			wantErr: false,
		},
		{
			name:    "null vector",
			input:   "null",
			want:    pgtype.Vector{},
			wantErr: false,
		},
		{
			name:    "vector with decimals",
			input:   "[1.5,2.25,3.75]",
			want:    pgtype.Vector{Vec: []float32{1.5, 2.25, 3.75}, Valid: true},
			wantErr: false,
		},
		{
			name:    "vector with spaces",
			input:   "[ 1.5 , 2.25 , 3.75 ]",
			want:    pgtype.Vector{Vec: []float32{1.5, 2.25, 3.75}, Valid: true},
			wantErr: false,
		},
		{
			name:    "invalid format - no brackets",
			input:   "1,2,3",
			wantErr: true,
		},
		{
			name:    "invalid format - not a number",
			input:   "[1,2,abc]",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got pgtype.Vector
			err := got.UnmarshalJSON([]byte(tt.input))
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want.Valid, got.Valid)
				if tt.want.Valid {
					require.Equal(t, tt.want.Vec, got.Vec)
				}
			}
		})
	}
}

func TestVectorString(t *testing.T) {
	tests := []struct {
		name   string
		vector pgtype.Vector
		want   string
	}{
		{
			name:   "valid vector",
			vector: pgtype.Vector{Vec: []float32{1, 2, 3}, Valid: true},
			want:   "[1,2,3]",
		},
		{
			name:   "empty vector",
			vector: pgtype.Vector{Vec: []float32{}, Valid: true},
			want:   "[]",
		},
		{
			name:   "null vector",
			vector: pgtype.Vector{},
			want:   "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.vector.String()
			require.Equal(t, tt.want, got)
		})
	}
}

func TestVectorCodecEncodeBinary(t *testing.T) {
	tests := []struct {
		name    string
		vector  pgtype.Vector
		want    []byte
		wantNil bool
	}{
		{
			name:   "valid vector",
			vector: pgtype.Vector{Vec: []float32{1, 2, 3}, Valid: true},
			want: func() []byte {
				buf := make([]byte, 0)
				buf = binary.BigEndian.AppendUint16(buf, 3)
				buf = binary.BigEndian.AppendUint16(buf, 0)
				buf = binary.BigEndian.AppendUint32(buf, math.Float32bits(1))
				buf = binary.BigEndian.AppendUint32(buf, math.Float32bits(2))
				buf = binary.BigEndian.AppendUint32(buf, math.Float32bits(3))
				return buf
			}(),
		},
		{
			name:   "empty vector",
			vector: pgtype.Vector{Vec: []float32{}, Valid: true},
			want: func() []byte {
				buf := make([]byte, 0)
				buf = binary.BigEndian.AppendUint16(buf, 0)
				buf = binary.BigEndian.AppendUint16(buf, 0)
				return buf
			}(),
		},
		{
			name:    "null vector",
			vector:  pgtype.Vector{},
			wantNil: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			codec := pgtype.VectorCodec{}
			plan := codec.PlanEncode(nil, 0, pgtype.BinaryFormatCode, tt.vector)
			require.NotNil(t, plan)

			got, err := plan.Encode(tt.vector, nil)
			require.NoError(t, err)

			if tt.wantNil {
				require.Nil(t, got)
			} else {
				require.Equal(t, tt.want, got)
			}
		})
	}
}

func TestVectorCodecDecodeTextFormat(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    pgtype.Vector
		wantErr bool
	}{
		{
			name:  "valid vector",
			input: "[1,2,3]",
			want:  pgtype.Vector{Vec: []float32{1, 2, 3}, Valid: true},
		},
		{
			name:  "empty vector",
			input: "[]",
			want:  pgtype.Vector{Vec: []float32{}, Valid: true},
		},
		{
			name:  "vector with decimals",
			input: "[1.5,2.25,3.75]",
			want:  pgtype.Vector{Vec: []float32{1.5, 2.25, 3.75}, Valid: true},
		},
		{
			name:    "invalid format",
			input:   "1,2,3",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			codec := pgtype.VectorCodec{}
			var got pgtype.Vector
			plan := codec.PlanScan(nil, 0, pgtype.TextFormatCode, &got)
			require.NotNil(t, plan)

			err := plan.Scan([]byte(tt.input), &got)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want.Valid, got.Valid)
				if tt.want.Valid {
					require.Equal(t, tt.want.Vec, got.Vec)
				}
			}
		})
	}
}

func TestVectorCodecDecodeBinaryFormat(t *testing.T) {
	tests := []struct {
		name    string
		input   []byte
		want    pgtype.Vector
		wantErr bool
	}{
		{
			name: "valid vector",
			input: func() []byte {
				buf := make([]byte, 0)
				buf = binary.BigEndian.AppendUint16(buf, 3)
				buf = binary.BigEndian.AppendUint16(buf, 0)
				buf = binary.BigEndian.AppendUint32(buf, math.Float32bits(1))
				buf = binary.BigEndian.AppendUint32(buf, math.Float32bits(2))
				buf = binary.BigEndian.AppendUint32(buf, math.Float32bits(3))
				return buf
			}(),
			want: pgtype.Vector{Vec: []float32{1, 2, 3}, Valid: true},
		},
		{
			name: "empty vector",
			input: func() []byte {
				buf := make([]byte, 0)
				buf = binary.BigEndian.AppendUint16(buf, 0)
				buf = binary.BigEndian.AppendUint16(buf, 0)
				return buf
			}(),
			want: pgtype.Vector{Vec: []float32{}, Valid: true},
		},
		{
			name:    "invalid length",
			input:   []byte{0, 0},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			codec := pgtype.VectorCodec{}
			var got pgtype.Vector
			plan := codec.PlanScan(nil, 0, pgtype.BinaryFormatCode, &got)
			require.NotNil(t, plan)

			err := plan.Scan(tt.input, &got)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want.Valid, got.Valid)
				if tt.want.Valid {
					require.True(t, reflect.DeepEqual(tt.want.Vec, got.Vec))
				}
			}
		})
	}
}

func TestVectorScan(t *testing.T) {
	tests := []struct {
		name    string
		input   any
		want    pgtype.Vector
		wantErr bool
	}{
		{
			name:  "string input",
			input: "[1,2,3]",
			want:  pgtype.Vector{Vec: []float32{1, 2, 3}, Valid: true},
		},
		{
			name:  "byte slice input",
			input: []byte("[1,2,3]"),
			want:  pgtype.Vector{Vec: []float32{1, 2, 3}, Valid: true},
		},
		{
			name:  "nil input",
			input: nil,
			want:  pgtype.Vector{},
		},
		{
			name:    "invalid type",
			input:   123,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got pgtype.Vector
			err := got.Scan(tt.input)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want.Valid, got.Valid)
				if tt.want.Valid {
					require.Equal(t, tt.want.Vec, got.Vec)
				}
			}
		})
	}
}

func TestVectorValue(t *testing.T) {
	tests := []struct {
		name    string
		vector  pgtype.Vector
		want    string
		wantNil bool
	}{
		{
			name:   "valid vector",
			vector: pgtype.Vector{Vec: []float32{1, 2, 3}, Valid: true},
			want:   "[1,2,3]",
		},
		{
			name:    "null vector",
			vector:  pgtype.Vector{},
			wantNil: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.vector.Value()
			require.NoError(t, err)

			if tt.wantNil {
				require.Nil(t, got)
			} else {
				require.Equal(t, tt.want, got)
			}
		})
	}
}
