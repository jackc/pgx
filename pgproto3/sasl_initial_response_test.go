package pgproto3_test

import (
	"encoding/binary"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

// saslInitialResponseSrc builds a SASLInitialResponse body with the given
// declared initial-response length and trailing data bytes.
func saslInitialResponseSrc(declaredLen int32, data []byte) []byte {
	src := []byte("SCRAM-SHA-256")
	src = append(src, 0)
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(declaredLen))
	src = append(src, lenBuf[:]...)
	src = append(src, data...)
	return src
}

func TestSASLInitialResponseDecodeValid(t *testing.T) {
	t.Parallel()

	data := []byte("n,,n=,r=abcdef")
	var msg pgproto3.SASLInitialResponse
	err := msg.Decode(saslInitialResponseSrc(int32(len(data)), data))
	require.NoError(t, err)
	require.Equal(t, "SCRAM-SHA-256", msg.AuthMechanism)
	require.Equal(t, data, msg.Data)
}

func TestSASLInitialResponseDecodeNoInitialResponse(t *testing.T) {
	t.Parallel()

	// A declared length of -1 means the mechanism sends no initial
	// response. The message must end immediately after the length field
	// and Data must be empty.
	var msg pgproto3.SASLInitialResponse
	err := msg.Decode(saslInitialResponseSrc(-1, nil))
	require.NoError(t, err)
	require.Equal(t, "SCRAM-SHA-256", msg.AuthMechanism)
	require.Empty(t, msg.Data)
}

func TestSASLInitialResponseDecodeLengthMismatch(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name        string
		declaredLen int32
		data        []byte
	}{
		{"declared longer than actual", 10, []byte("abc")},
		{"declared shorter than actual", 2, []byte("abcdef")},
		{"declared zero with trailing data", 0, []byte("abc")},
		{"no initial response with trailing data", -1, []byte("abc")},
		{"negative length other than -1", -2, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var msg pgproto3.SASLInitialResponse
			err := msg.Decode(saslInitialResponseSrc(tc.declaredLen, tc.data))
			require.Error(t, err, "Decode should reject declared length %d with %d data bytes", tc.declaredLen, len(tc.data))
		})
	}
}

func TestSASLInitialResponseEncodeDecodeRoundTrip(t *testing.T) {
	t.Parallel()

	original := pgproto3.SASLInitialResponse{
		AuthMechanism: "SCRAM-SHA-256",
		Data:          []byte("n,,n=,r=abcdef"),
	}
	encoded, err := original.Encode(nil)
	require.NoError(t, err)

	// Strip the message type byte and length prefix that Encode adds.
	var decoded pgproto3.SASLInitialResponse
	err = decoded.Decode(encoded[5:])
	require.NoError(t, err)
	require.Equal(t, original.AuthMechanism, decoded.AuthMechanism)
	require.Equal(t, original.Data, decoded.Data)
}
