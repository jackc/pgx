package pgtype_test

import (
	"context"
	"math"
	"testing"

	"github.com/jackc/pgx/v5/pgxtest"
)

func TestQcharTranscode(t *testing.T) {
	skipCockroachDB(t, "Server does not support qchar")

	var tests []pgxtest.ValueRoundTripTest
	for i := 0; i <= math.MaxUint8; i++ {
		tests = append(tests, pgxtest.ValueRoundTripTest{rune(i), new(rune), isExpectedEq(rune(i))})
		tests = append(tests, pgxtest.ValueRoundTripTest{byte(i), new(byte), isExpectedEq(byte(i))})
	}
	tests = append(tests, pgxtest.ValueRoundTripTest{nil, new(*rune), isExpectedEq((*rune)(nil))})
	tests = append(tests, pgxtest.ValueRoundTripTest{nil, new(*byte), isExpectedEq((*byte)(nil))})

	// Can only test with known OIDs as rune and byte would be considered numbers.
	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, pgxtest.KnownOIDQueryExecModes, `"char"`, tests)
}
