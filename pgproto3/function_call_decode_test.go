package pgproto3_test

import (
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

// github.com/jackc/pgx/issues/2520
func TestFunctionCallDecodeNegativeArgumentLength(t *testing.T) {
	t.Parallel()

	// Craft a FunctionCall message with a negative argument length that is not -1.
	//
	// Message layout:
	//   - function OID: 1 (4 bytes)
	//   - arg format code count: 0 (2 bytes)
	//   - argument count: 1 (2 bytes)
	//   - argument 0 length: -2 (4 bytes, 0xFFFFFFFE)
	src := []byte{
		0, 0, 0, 1, // function OID = 1
		0, 0, // arg format code count = 0
		0, 1, // argument count = 1
		0xFF, 0xFF, 0xFF, 0xFE, // argument length = -2
	}

	var msg pgproto3.FunctionCall
	err := msg.Decode(src)
	require.Error(t, err, "FunctionCall.Decode should reject negative argument length other than -1")
}
