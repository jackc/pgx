package zeronull_test

import (
	"testing"

	"github.com/jackc/pgtype/testutil"
	"github.com/jackc/pgtype/zeronull"
)

func TestUUIDTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscode(t, "uuid", []interface{}{
		(*zeronull.UUID)(&[16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}),
		(*zeronull.UUID)(&[16]byte{}),
	})
}

func TestUUIDConvertsGoZeroToNull(t *testing.T) {
	testutil.TestGoZeroToNullConversion(t, "uuid", (*zeronull.UUID)(&[16]byte{}))
}

func TestUUIDConvertsNullToGoZero(t *testing.T) {
	testutil.TestNullToGoZeroConversion(t, "uuid", (*zeronull.UUID)(&[16]byte{}))
}
