package pgtype_test

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/jackc/pgx/v4/pgtype"
	"github.com/jackc/pgx/v4/pgtype/testutil"
	"github.com/stretchr/testify/require"
)

func TestUUIDTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscode(t, "uuid", []interface{}{
		&pgtype.UUID{Bytes: [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, Valid: true},
		&pgtype.UUID{},
	})
}

type SomeUUIDWrapper struct {
	SomeUUIDType
}

type SomeUUIDType [16]byte

func TestUUIDSet(t *testing.T) {
	successfulTests := []struct {
		source interface{}
		result pgtype.UUID
	}{
		{
			source: nil,
			result: pgtype.UUID{},
		},
		{
			source: [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			result: pgtype.UUID{Bytes: [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, Valid: true},
		},
		{
			source: []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			result: pgtype.UUID{Bytes: [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, Valid: true},
		},
		{
			source: SomeUUIDType{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			result: pgtype.UUID{Bytes: [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, Valid: true},
		},
		{
			source: ([]byte)(nil),
			result: pgtype.UUID{},
		},
		{
			source: "00010203-0405-0607-0809-0a0b0c0d0e0f",
			result: pgtype.UUID{Bytes: [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, Valid: true},
		},
		{
			source: "000102030405060708090a0b0c0d0e0f",
			result: pgtype.UUID{Bytes: [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, Valid: true},
		},
	}

	for i, tt := range successfulTests {
		var r pgtype.UUID
		err := r.Set(tt.source)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if r.Bytes != tt.result.Bytes || r.Valid != tt.result.Valid {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.source, tt.result, r)
		}
	}
}

func TestUUIDAssignTo(t *testing.T) {
	{
		src := pgtype.UUID{Bytes: [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, Valid: true}
		var dst [16]byte
		expected := [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}

		err := src.AssignTo(&dst)
		if err != nil {
			t.Error(err)
		}

		if dst != expected {
			t.Errorf("expected %v to assign %v, but result was %v", src, expected, dst)
		}
	}

	{
		src := pgtype.UUID{Bytes: [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, Valid: true}
		var dst []byte
		expected := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}

		err := src.AssignTo(&dst)
		if err != nil {
			t.Error(err)
		}

		if bytes.Compare(dst, expected) != 0 {
			t.Errorf("expected %v to assign %v, but result was %v", src, expected, dst)
		}
	}

	{
		src := pgtype.UUID{Bytes: [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, Valid: true}
		var dst SomeUUIDType
		expected := [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}

		err := src.AssignTo(&dst)
		if err != nil {
			t.Error(err)
		}

		if dst != expected {
			t.Errorf("expected %v to assign %v, but result was %v", src, expected, dst)
		}
	}

	{
		src := pgtype.UUID{Bytes: [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, Valid: true}
		var dst string
		expected := "00010203-0405-0607-0809-0a0b0c0d0e0f"

		err := src.AssignTo(&dst)
		if err != nil {
			t.Error(err)
		}

		if dst != expected {
			t.Errorf("expected %v to assign %v, but result was %v", src, expected, dst)
		}
	}

	{
		src := pgtype.UUID{Bytes: [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, Valid: true}
		var dst SomeUUIDWrapper
		expected := [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}

		err := src.AssignTo(&dst)
		if err != nil {
			t.Error(err)
		}

		if dst.SomeUUIDType != expected {
			t.Errorf("expected %v to assign %v, but result was %v", src, expected, dst)
		}
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
