// Code generated from pgtype/zeronull/int_test.go.erb. DO NOT EDIT.

package zeronull_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgtype/zeronull"
	"github.com/jackc/pgx/v5/pgxtest"
)

func TestInt2Transcode(t *testing.T) {
	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "int2", []pgxtest.ValueRoundTripTest{
		{
			Param:  (zeronull.Int2)(1),
			Result: new(zeronull.Int2),
			Test:   isExpectedEq((zeronull.Int2)(1)),
		},
		{
			Param:  nil,
			Result: new(zeronull.Int2),
			Test:   isExpectedEq((zeronull.Int2)(0)),
		},
		{
			Param:  (zeronull.Int2)(0),
			Result: new(any),
			Test:   isExpectedEq(nil),
		},
	})
}

func TestInt4Transcode(t *testing.T) {
	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "int4", []pgxtest.ValueRoundTripTest{
		{
			(zeronull.Int4)(1),
			new(zeronull.Int4),
			isExpectedEq((zeronull.Int4)(1)),
		},
		{
			nil,
			new(zeronull.Int4),
			isExpectedEq((zeronull.Int4)(0)),
		},
		{
			(zeronull.Int4)(0),
			new(any),
			isExpectedEq(nil),
		},
	})
}

func TestInt8Transcode(t *testing.T) {
	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "int8", []pgxtest.ValueRoundTripTest{
		{
			(zeronull.Int8)(1),
			new(zeronull.Int8),
			isExpectedEq((zeronull.Int8)(1)),
		},
		{
			nil,
			new(zeronull.Int8),
			isExpectedEq((zeronull.Int8)(0)),
		},
		{
			(zeronull.Int8)(0),
			new(any),
			isExpectedEq(nil),
		},
	})
}
