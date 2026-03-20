package pgproto3_test

import (
	"bytes"
	"testing"

	"github.com/jackc/pgx/v5/internal/pgio"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

func FuzzFrontend(f *testing.F) {
	testcases := []struct {
		msgType byte
		msgLen  uint32
		msgBody []byte
	}{
		{
			msgType: 'Z',
			msgLen:  2,
			msgBody: []byte{'I'},
		},
		{
			msgType: 'Z',
			msgLen:  5,
			msgBody: []byte{'I'},
		},
	}
	for _, tc := range testcases {
		f.Add(tc.msgType, tc.msgLen, tc.msgBody)
	}
	f.Fuzz(func(t *testing.T, msgType byte, msgLen uint32, msgBody []byte) {
		// Prune any msgLen > len(msgBody) because they would hang the test waiting for more input.
		if int(msgLen) > len(msgBody)+4 {
			return
		}

		// Prune any messages that are too long.
		if msgLen > 128 || len(msgBody) > 128 {
			return
		}

		r := &bytes.Buffer{}
		w := &bytes.Buffer{}
		fe := pgproto3.NewFrontend(r, w)

		var encodedMsg []byte
		encodedMsg = append(encodedMsg, msgType)
		encodedMsg = pgio.AppendUint32(encodedMsg, msgLen)
		encodedMsg = append(encodedMsg, msgBody...)
		_, err := r.Write(encodedMsg)
		require.NoError(t, err)

		// Not checking anything other than no panic.
		fe.Receive()
	})
}

func FuzzBackend(f *testing.F) {
	testcases := []struct {
		msgType byte
		msgLen  uint32
		msgBody []byte
	}{
		{msgType: 'B', msgLen: 14, msgBody: []byte{0, 0, 0, 0, 0, 1, 0, 0, 0, 1}},             // Bind
		{msgType: 'P', msgLen: 8, msgBody: []byte{0, 0, 0, 0}},                                // Parse
		{msgType: 'Q', msgLen: 6, msgBody: []byte{0}},                                         // Query
		{msgType: 'F', msgLen: 18, msgBody: []byte{0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1}}, // FunctionCall
	}
	for _, tc := range testcases {
		f.Add(tc.msgType, tc.msgLen, tc.msgBody)
	}
	f.Fuzz(func(t *testing.T, msgType byte, msgLen uint32, msgBody []byte) {
		// Prune any msgLen > len(msgBody) because they would hang the test waiting for more input.
		if int(msgLen) > len(msgBody)+4 {
			return
		}

		// Prune any messages that are too long.
		if msgLen > 128 || len(msgBody) > 128 {
			return
		}

		r := &bytes.Buffer{}
		w := &bytes.Buffer{}
		be := pgproto3.NewBackend(r, w)

		var encodedMsg []byte
		encodedMsg = append(encodedMsg, msgType)
		encodedMsg = pgio.AppendUint32(encodedMsg, msgLen)
		encodedMsg = append(encodedMsg, msgBody...)
		_, err := r.Write(encodedMsg)
		require.NoError(t, err)

		// Not checking anything other than no panic.
		be.Receive()
	})
}

// Fuzz individual Decode methods directly. This provides better coverage than
// going through Frontend/Backend.Receive because there is no message framing
// overhead and the fuzzer can explore the full input space of each decoder.

func FuzzBind(f *testing.F) {
	f.Add([]byte{0, 0, 0, 0, 0, 1, 0, 0, 0, 1})
	f.Add([]byte{0, 0, 0, 0, 0, 1, 0xFF, 0xFF, 0xFF, 0xFF}) // NULL param
	f.Add([]byte{0, 0, 0, 0, 0, 1, 0xFF, 0xFF, 0xFF, 0xFE}) // negative param length
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		var msg pgproto3.Bind
		msg.Decode(data) // must not panic
	})
}

func FuzzDataRow(f *testing.F) {
	f.Add([]byte{0, 1, 0, 0, 0, 3, 'a', 'b', 'c'})
	f.Add([]byte{0, 1, 0xFF, 0xFF, 0xFF, 0xFF}) // NULL
	f.Add([]byte{0, 1, 0xFF, 0xFF, 0xFF, 0xFE}) // negative length
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		var msg pgproto3.DataRow
		msg.Decode(data) // must not panic
	})
}

func FuzzRowDescription(f *testing.F) {
	f.Add([]byte{0, 1, 'n', 'a', 'm', 'e', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
	f.Add([]byte{0, 0})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		var msg pgproto3.RowDescription
		msg.Decode(data) // must not panic
	})
}

func FuzzFunctionCall(f *testing.F) {
	f.Add([]byte{0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1})
	f.Add([]byte{0, 0, 0, 1, 0, 0, 0, 1, 0xFF, 0xFF, 0xFF, 0xFF, 0, 1}) // NULL arg
	f.Add([]byte{0, 0, 0, 1, 0, 0, 0, 1, 0xFF, 0xFF, 0xFF, 0xFE, 0, 1}) // negative arg length
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		var msg pgproto3.FunctionCall
		msg.Decode(data) // must not panic
	})
}

func FuzzFunctionCallResponse(f *testing.F) {
	f.Add([]byte{0, 0, 0, 3, 'a', 'b', 'c'})
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF}) // NULL
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFE}) // negative length
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		var msg pgproto3.FunctionCallResponse
		msg.Decode(data) // must not panic
	})
}

func FuzzParse(f *testing.F) {
	f.Add([]byte{0, 0, 0, 0})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		var msg pgproto3.Parse
		msg.Decode(data) // must not panic
	})
}

func FuzzQuery(f *testing.F) {
	f.Add([]byte("SELECT 1\x00"))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		var msg pgproto3.Query
		msg.Decode(data) // must not panic
	})
}

func FuzzClose(f *testing.F) {
	f.Add([]byte{'S', 't', 'e', 's', 't', 0})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		var msg pgproto3.Close
		msg.Decode(data) // must not panic
	})
}

func FuzzDescribe(f *testing.F) {
	f.Add([]byte{'S', 't', 'e', 's', 't', 0})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		var msg pgproto3.Describe
		msg.Decode(data) // must not panic
	})
}

func FuzzExecute(f *testing.F) {
	f.Add([]byte{'t', 'e', 's', 't', 0, 0, 0, 0, 0})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		var msg pgproto3.Execute
		msg.Decode(data) // must not panic
	})
}

func FuzzCopyInResponse(f *testing.F) {
	f.Add([]byte{0, 0, 1, 0, 0})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		var msg pgproto3.CopyInResponse
		msg.Decode(data) // must not panic
	})
}

func FuzzCopyOutResponse(f *testing.F) {
	f.Add([]byte{0, 0, 1, 0, 0})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		var msg pgproto3.CopyOutResponse
		msg.Decode(data) // must not panic
	})
}

func FuzzCopyBothResponse(f *testing.F) {
	f.Add([]byte{0, 0, 1, 0, 0})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		var msg pgproto3.CopyBothResponse
		msg.Decode(data) // must not panic
	})
}

func FuzzErrorResponse(f *testing.F) {
	f.Add([]byte{'S', 'E', 'R', 'R', 'O', 'R', 0, 'M', 't', 'e', 's', 't', 0, 0})
	f.Add([]byte{0})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		var msg pgproto3.ErrorResponse
		msg.Decode(data) // must not panic
	})
}

func FuzzNotificationResponse(f *testing.F) {
	f.Add([]byte{0, 0, 0, 1, 'c', 'h', 0, 'p', 0})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		var msg pgproto3.NotificationResponse
		msg.Decode(data) // must not panic
	})
}

func FuzzParameterDescription(f *testing.F) {
	f.Add([]byte{0, 1, 0, 0, 0, 23})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		var msg pgproto3.ParameterDescription
		msg.Decode(data) // must not panic
	})
}

func FuzzParameterStatus(f *testing.F) {
	f.Add([]byte{'k', 'e', 'y', 0, 'v', 'a', 'l', 0})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		var msg pgproto3.ParameterStatus
		msg.Decode(data) // must not panic
	})
}

func FuzzReadyForQuery(f *testing.F) {
	f.Add([]byte{'I'})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		var msg pgproto3.ReadyForQuery
		msg.Decode(data) // must not panic
	})
}

func FuzzSASLInitialResponse(f *testing.F) {
	f.Add([]byte("SCRAM-SHA-256\x00\x00\x00\x00\x04test"))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		var msg pgproto3.SASLInitialResponse
		msg.Decode(data) // must not panic
	})
}

func FuzzStartupMessage(f *testing.F) {
	f.Add([]byte{0, 3, 0, 0, 'u', 's', 'e', 'r', 0, 'p', 'g', 0, 0})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		var msg pgproto3.StartupMessage
		msg.Decode(data) // must not panic
	})
}

func FuzzNegotiateProtocolVersion(f *testing.F) {
	f.Add([]byte{0, 0, 0, 1, 0, 0, 0, 1, 'o', 'p', 't', 0})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		var msg pgproto3.NegotiateProtocolVersion
		msg.Decode(data) // must not panic
	})
}
