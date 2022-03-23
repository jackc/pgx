package pgtype_test

import (
	"math"
	"testing"

	"github.com/jackc/pgx/v5/pgtype/testutil"
)

func TestQcharTranscode(t *testing.T) {
	skipCockroachDB(t, "Server does not support qchar")

	var tests []testutil.TranscodeTestCase
	for i := 0; i <= math.MaxUint8; i++ {
		tests = append(tests, testutil.TranscodeTestCase{rune(i), new(rune), isExpectedEq(rune(i))})
		tests = append(tests, testutil.TranscodeTestCase{byte(i), new(byte), isExpectedEq(byte(i))})
	}
	tests = append(tests, testutil.TranscodeTestCase{nil, new(*rune), isExpectedEq((*rune)(nil))})
	tests = append(tests, testutil.TranscodeTestCase{nil, new(*byte), isExpectedEq((*byte)(nil))})

	testutil.RunTranscodeTests(t, `"char"`, tests)
}
