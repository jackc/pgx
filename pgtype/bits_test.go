package pgtype_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxtest"
)

func isExpectedEqBits(a any) func(any) bool {
	return func(v any) bool {
		ab := a.(pgtype.Bits)
		vb := v.(pgtype.Bits)
		return bytes.Equal(ab.Bytes, vb.Bytes) && ab.Len == vb.Len && ab.Valid == vb.Valid
	}
}

func TestBitsCodecBit(t *testing.T) {
	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "bit(40)", []pgxtest.ValueRoundTripTest{
		{
			Param:  pgtype.Bits{Bytes: []byte{0, 0, 0, 0, 0}, Len: 40, Valid: true},
			Result: new(pgtype.Bits),
			Test:   isExpectedEqBits(pgtype.Bits{Bytes: []byte{0, 0, 0, 0, 0}, Len: 40, Valid: true}),
		},
		{
			Param:  pgtype.Bits{Bytes: []byte{0, 1, 128, 254, 255}, Len: 40, Valid: true},
			Result: new(pgtype.Bits),
			Test:   isExpectedEqBits(pgtype.Bits{Bytes: []byte{0, 1, 128, 254, 255}, Len: 40, Valid: true}),
		},
		{Param: pgtype.Bits{}, Result: new(pgtype.Bits), Test: isExpectedEqBits(pgtype.Bits{})},
		{Param: nil, Result: new(pgtype.Bits), Test: isExpectedEqBits(pgtype.Bits{})},
	})
}

func TestBitsCodecVarbit(t *testing.T) {
	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "varbit", []pgxtest.ValueRoundTripTest{
		{
			Param:  pgtype.Bits{Bytes: []byte{}, Len: 0, Valid: true},
			Result: new(pgtype.Bits),
			Test:   isExpectedEqBits(pgtype.Bits{Bytes: []byte{}, Len: 0, Valid: true}),
		},
		{
			Param:  pgtype.Bits{Bytes: []byte{0, 1, 128, 254, 255}, Len: 40, Valid: true},
			Result: new(pgtype.Bits),
			Test:   isExpectedEqBits(pgtype.Bits{Bytes: []byte{0, 1, 128, 254, 255}, Len: 40, Valid: true}),
		},
		{
			Param:  pgtype.Bits{Bytes: []byte{0, 1, 128, 254, 128}, Len: 33, Valid: true},
			Result: new(pgtype.Bits),
			Test:   isExpectedEqBits(pgtype.Bits{Bytes: []byte{0, 1, 128, 254, 128}, Len: 33, Valid: true}),
		},
		{Param: pgtype.Bits{}, Result: new(pgtype.Bits), Test: isExpectedEqBits(pgtype.Bits{})},
		{Param: nil, Result: new(pgtype.Bits), Test: isExpectedEqBits(pgtype.Bits{})},
	})
}
