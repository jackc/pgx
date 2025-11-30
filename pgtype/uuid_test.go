package pgtype_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxtest"
	"github.com/stretchr/testify/require"
)

type renamedUUIDByteArray [16]byte

func TestUUIDCodec(t *testing.T) {
	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "uuid", []pgxtest.ValueRoundTripTest{
		{
			Param:  pgtype.UUID{Bytes: [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, Valid: true},
			Result: new(pgtype.UUID),
			Test:   isExpectedEq(pgtype.UUID{Bytes: [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, Valid: true}),
		},
		{
			Param:  "00010203-0405-0607-0809-0a0b0c0d0e0f",
			Result: new(pgtype.UUID),
			Test:   isExpectedEq(pgtype.UUID{Bytes: [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, Valid: true}),
		},
		{
			Param:  "000102030405060708090a0b0c0d0e0f",
			Result: new(pgtype.UUID),
			Test:   isExpectedEq(pgtype.UUID{Bytes: [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, Valid: true}),
		},
		{
			Param:  pgtype.UUID{Bytes: [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, Valid: true},
			Result: new(string),
			Test:   isExpectedEq("00010203-0405-0607-0809-0a0b0c0d0e0f"),
		},
		{Param: pgtype.UUID{}, Result: new([]byte), Test: isExpectedEqBytes([]byte(nil))},
		{Param: pgtype.UUID{}, Result: new(pgtype.UUID), Test: isExpectedEq(pgtype.UUID{})},
		{Param: nil, Result: new(pgtype.UUID), Test: isExpectedEq(pgtype.UUID{})},
	})

	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, pgxtest.KnownOIDQueryExecModes, "uuid", []pgxtest.ValueRoundTripTest{
		{
			Param:  [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			Result: new(pgtype.UUID),
			Test:   isExpectedEq(pgtype.UUID{Bytes: [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, Valid: true}),
		},
		{
			Param:  renamedUUIDByteArray{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			Result: new(pgtype.UUID),
			Test:   isExpectedEq(pgtype.UUID{Bytes: [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, Valid: true}),
		},
		{
			Param:  []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			Result: new(renamedUUIDByteArray),
			Test:   isExpectedEq(renamedUUIDByteArray{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}),
		},
		{
			Param:  []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			Result: new(pgtype.UUID),
			Test:   isExpectedEq(pgtype.UUID{Bytes: [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, Valid: true}),
		},
	})
}

func TestUUID_String(t *testing.T) {
	tests := []struct {
		name string
		src  pgtype.UUID
		want string
	}{
		{
			name: "first",
			src: pgtype.UUID{
				Bytes: [16]byte{29, 72, 90, 122, 109, 24, 69, 153, 140, 108, 52, 66, 86, 22, 136, 122},
				Valid: true,
			},
			want: "1d485a7a-6d18-4599-8c6c-34425616887a",
		},
		{
			name: "third",
			src: pgtype.UUID{
				Bytes: [16]byte{},
			},
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.src.String()
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("MarshalJSON() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestUUID_MarshalJSON(t *testing.T) {
	tests := []struct {
		name string
		src  pgtype.UUID
		want []byte
	}{
		{
			name: "first",
			src: pgtype.UUID{
				Bytes: [16]byte{29, 72, 90, 122, 109, 24, 69, 153, 140, 108, 52, 66, 86, 22, 136, 122},
				Valid: true,
			},
			want: []byte(`"1d485a7a-6d18-4599-8c6c-34425616887a"`),
		},
		{
			name: "third",
			src: pgtype.UUID{
				Bytes: [16]byte{},
			},
			want: []byte("null"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.src.MarshalJSON()
			require.NoError(t, err)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("MarshalJSON() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestUUID_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name    string
		want    *pgtype.UUID
		src     []byte
		wantErr bool
	}{
		{
			name: "first",
			want: &pgtype.UUID{
				Bytes: [16]byte{29, 72, 90, 122, 109, 24, 69, 153, 140, 108, 52, 66, 86, 22, 136, 122},
				Valid: true,
			},
			src:     []byte(`"1d485a7a-6d18-4599-8c6c-34425616887a"`),
			wantErr: false,
		},
		{
			name: "second",
			want: &pgtype.UUID{
				Bytes: [16]byte{},
			},
			src:     []byte("null"),
			wantErr: false,
		},
		{
			name: "third",
			want: &pgtype.UUID{
				Bytes: [16]byte{},
				Valid: false,
			},
			src:     []byte("1d485a7a-6d18-4599-8c6c-34425616887a"),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := &pgtype.UUID{}
			if err := got.UnmarshalJSON(tt.src); (err != nil) != tt.wantErr {
				t.Errorf("UnmarshalJSON() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("UnmarshalJSON() got = %v, want %v", got, tt.want)
			}
		})
	}
}
