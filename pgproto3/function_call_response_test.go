package pgproto3_test

import (
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

func TestFunctionCallResponseDecodeNullResult(t *testing.T) {
	t.Parallel()

	// A valid NULL function call response has result size = -1 (0xFFFFFFFF).
	// Without the int32 cast, int(uint32(0xFFFFFFFF)) is 4294967295 on 64-bit,
	// so the -1 sentinel check is dead code and a valid NULL is incorrectly rejected.
	src := []byte{
		0xFF, 0xFF, 0xFF, 0xFF, // result size = -1 (NULL)
	}

	var msg pgproto3.FunctionCallResponse
	err := msg.Decode(src)
	require.NoError(t, err, "FunctionCallResponse.Decode should accept -1 as NULL result")
	require.Nil(t, msg.Result)
}

func TestFunctionCallResponseDecodeNegativeResultSize(t *testing.T) {
	t.Parallel()

	// A result size that is negative but not -1 should be rejected.
	src := []byte{
		0xFF, 0xFF, 0xFF, 0xFE, // result size = -2
	}

	var msg pgproto3.FunctionCallResponse
	err := msg.Decode(src)
	require.Error(t, err, "FunctionCallResponse.Decode should reject negative result size other than -1")
}
