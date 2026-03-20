package pgproto3_test

import (
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

// https://github.com/jackc/pgx/issues/2519
func TestBindDecodeNegativeParameterLength(t *testing.T) {
	t.Parallel()

	// Craft a Bind message with a negative parameter length that is not -1.
	// This should return an error, not panic.
	//
	// Message layout:
	//   - destination portal: "" (1 byte null terminator)
	//   - prepared statement: "" (1 byte null terminator)
	//   - parameter format code count: 0 (2 bytes)
	//   - parameter count: 1 (2 bytes)
	//   - parameter 0 length: -2 (4 bytes, 0xFFFFFFFE)
	src := []byte{
		0,    // destination portal null terminator
		0,    // prepared statement null terminator
		0, 0, // parameter format code count = 0
		0, 1, // parameter count = 1
		0xFF, 0xFF, 0xFF, 0xFE, // parameter length = -2
	}

	var bind pgproto3.Bind
	err := bind.Decode(src)
	require.Error(t, err, "Bind.Decode should reject negative parameter length other than -1")
}

func TestBindBiggerThanMaxMessageBodyLen(t *testing.T) {
	t.Parallel()

	// Maximum allowed size.
	_, err := (&pgproto3.Bind{Parameters: [][]byte{make([]byte, pgproto3.MaxMessageBodyLen-16)}}).Encode(nil)
	require.NoError(t, err)

	// 1 byte too big
	_, err = (&pgproto3.Bind{Parameters: [][]byte{make([]byte, pgproto3.MaxMessageBodyLen-15)}}).Encode(nil)
	require.Error(t, err)
}
