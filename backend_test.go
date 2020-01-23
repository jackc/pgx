package pgproto3_test

import (
	"testing"

	"github.com/jackc/pgio"
	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/require"
)

func TestBackendReceiveInterrupted(t *testing.T) {
	t.Parallel()

	server := &interruptReader{}
	server.push([]byte{'Q', 0, 0, 0, 6})

	backend := pgproto3.NewBackend(pgproto3.NewChunkReader(server), nil)

	msg, err := backend.Receive()
	if err == nil {
		t.Fatal("expected err")
	}
	if msg != nil {
		t.Fatalf("did not expect msg, but %v", msg)
	}

	server.push([]byte{'I', 0})

	msg, err = backend.Receive()
	if err != nil {
		t.Fatal(err)
	}
	if msg, ok := msg.(*pgproto3.Query); !ok || msg.String != "I" {
		t.Fatalf("unexpected msg: %v", msg)
	}
}

func TestStartupMessage(t *testing.T) {
	t.Parallel()

	t.Run("valid StartupMessage", func(t *testing.T) {
		want := &pgproto3.StartupMessage{
			ProtocolVersion: pgproto3.ProtocolVersionNumber,
			Parameters: map[string]string{
				"username": "tester",
			},
		}
		dst := []byte{}
		dst = want.Encode(dst)

		server := &interruptReader{}
		server.push(dst)

		backend := pgproto3.NewBackend(pgproto3.NewChunkReader(server), nil)

		msg, err := backend.ReceiveStartupMessage()
		require.NoError(t, err)
		require.Equal(t, want, msg)
	})

	t.Run("invalid packet length", func(t *testing.T) {
		wantErr := "invalid length of startup packet"
		tests := []struct {
			name      string
			packetLen uint32
		}{
			{
				name: "large packet length",
				// Since the StartupMessage contains the "Length of message contents
				//  in bytes, including self", the max startup packet length is actually
				//  10000+4. Therefore, let's go past the limit with 10005
				packetLen: 10005,
			},
			{
				name:      "short packet length",
				packetLen: 3,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				server := &interruptReader{}
				dst := []byte{}
				dst = pgio.AppendUint32(dst, tt.packetLen)
				dst = pgio.AppendUint32(dst, pgproto3.ProtocolVersionNumber)
				server.push(dst)

				backend := pgproto3.NewBackend(pgproto3.NewChunkReader(server), nil)

				msg, err := backend.ReceiveStartupMessage()
				require.Error(t, err)
				require.Nil(t, msg)
				require.Contains(t, err.Error(), wantErr)
			})
		}
	})

}
