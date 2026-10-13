package pgx

import (
	"encoding/binary"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
)

// arrayBinaryHeader returns ndim, flags and the (length, lower bound) pairs from a binary array.
func arrayBinaryHeader(t *testing.T, buf []byte) (uint32, uint32, [][2]int32) {
	t.Helper()
	require.GreaterOrEqual(t, len(buf), 12)
	ndim := binary.BigEndian.Uint32(buf[0:4])
	flags := binary.BigEndian.Uint32(buf[4:8])
	var dims [][2]int32
	for i := uint32(0); i < ndim; i++ {
		off := 12 + i*8
		require.GreaterOrEqual(t, len(buf), int(off)+8)
		dims = append(dims, [2]int32{
			int32(binary.BigEndian.Uint32(buf[off : off+4])),
			int32(binary.BigEndian.Uint32(buf[off+4 : off+8])),
		})
	}
	return ndim, flags, dims
}

// encodeCopyValue must preserve the dimensions of a multidimensional array given as a string.
// https://github.com/jackc/pgx/issues/2385
func TestEncodeCopyValuePreservesArrayDimensions(t *testing.T) {
	t.Parallel()

	m := pgtype.NewMap()
	const textArrayOID = uint32(1009) // text[]
	const int4OID = uint32(23)        // int4

	// encode returns the binary representation of a single copy-format value (length prefix removed).
	encode := func(t *testing.T, oid uint32, value any) []byte {
		t.Helper()
		buf, err := encodeCopyValue(m, nil, oid, value)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(buf), 4)
		length := int32(binary.BigEndian.Uint32(buf))
		require.EqualValues(t, len(buf)-4, length)
		return buf[4:]
	}

	t.Run("two-dimensional array string", func(t *testing.T) {
		ndim, flags, dims := arrayBinaryHeader(t, encode(t, textArrayOID, "{{1,2},{3,4}}"))
		require.EqualValues(t, 2, ndim)
		require.EqualValues(t, 0, flags)
		require.Equal(t, [][2]int32{{2, 1}, {2, 1}}, dims)
	})

	t.Run("one-dimensional array string", func(t *testing.T) {
		ndim, _, dims := arrayBinaryHeader(t, encode(t, textArrayOID, "{a,b,c}"))
		require.EqualValues(t, 1, ndim)
		require.Equal(t, [][2]int32{{3, 1}}, dims)
	})

	t.Run("array string with NULL element", func(t *testing.T) {
		binaryValue := encode(t, textArrayOID, "{a,NULL,b}")
		ndim, flags, dims := arrayBinaryHeader(t, binaryValue)
		require.EqualValues(t, 1, ndim)
		require.EqualValues(t, 1, flags) // ARRAY_HASNULL
		require.Equal(t, [][2]int32{{3, 1}}, dims)

		// Elements follow the header: int32 length (-1 for NULL) then the bytes.
		off := 12 + len(dims)*8
		elemLen := int32(binary.BigEndian.Uint32(binaryValue[off : off+4]))
		require.EqualValues(t, 1, elemLen)
		require.Equal(t, "a", string(binaryValue[off+4:off+4+int(elemLen)]))
		off += 4 + int(elemLen)
		elemLen = int32(binary.BigEndian.Uint32(binaryValue[off : off+4]))
		require.EqualValues(t, -1, elemLen) // NULL
		off += 4
		elemLen = int32(binary.BigEndian.Uint32(binaryValue[off : off+4]))
		require.EqualValues(t, 1, elemLen)
		require.Equal(t, "b", string(binaryValue[off+4:off+4+int(elemLen)]))
	})

	t.Run("invalid array string still errors", func(t *testing.T) {
		_, err := encodeCopyValue(m, nil, textArrayOID, "not an array")
		require.Error(t, err)
	})

	t.Run("non-array type unaffected", func(t *testing.T) {
		binaryValue := encode(t, int4OID, "42")
		require.Len(t, binaryValue, 4)
		require.EqualValues(t, 42, int32(binary.BigEndian.Uint32(binaryValue)))
	})
}
