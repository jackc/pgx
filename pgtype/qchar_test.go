package pgtype_test

import (
	"math"
	"testing"
)

func TestQcharTranscode(t *testing.T) {
	var tests []PgxTranscodeTestCase
	for i := 0; i <= math.MaxUint8; i++ {
		tests = append(tests, PgxTranscodeTestCase{rune(i), new(rune), isExpectedEq(rune(i))})
		tests = append(tests, PgxTranscodeTestCase{byte(i), new(byte), isExpectedEq(byte(i))})
	}
	tests = append(tests, PgxTranscodeTestCase{nil, new(*rune), isExpectedEq((*rune)(nil))})
	tests = append(tests, PgxTranscodeTestCase{nil, new(*byte), isExpectedEq((*byte)(nil))})

	testPgxCodec(t, `"char"`, tests)
}
