package pgproto3

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCancelRequestDecode(t *testing.T) {
	secretKey32 := []byte{
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10,
		0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
		0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F, 0x20,
	}

	tests := []struct {
		name              string
		src               []byte
		expectedProcessID uint32
		expectedSecretKey []byte
		expectError       bool
	}{
		{
			name: "Protocol 3.0 (16 bytes total)",
			src: []byte{
				0x04, 0xD2, 0x16, 0x2E, // cancelRequestCode: 80877102
				0x00, 0x00, 0x22, 0xA0, // ProcessID: 8864
				0xD9, 0x0C, 0xAE, 0xDB, // SecretKey
			},
			expectedProcessID: 8864,
			expectedSecretKey: []byte{0xD9, 0x0C, 0xAE, 0xDB},
		},
		{
			name: "Protocol 3.2 (variable-length 32-byte key)",
			src: append([]byte{
				0x04, 0xD2, 0x16, 0x2E, // cancelRequestCode: 80877102
				0x00, 0x00, 0x22, 0xA0, // ProcessID: 8864
			}, secretKey32...),
			expectedProcessID: 8864,
			expectedSecretKey: secretKey32,
		},
		{
			name: "invalid length (too short)",
			src: []byte{
				0x00, 0x00, 0x00, 0x00, // invalid length
				0x00, 0x00, 0x22, 0xA0, // ProcessID: 8864
				0xD9, 0x0C, 0xAE, 0xDB, // SecretKey
			},
			expectError: true,
		},
		{
			name: "invalid length (too long)",
			src: append([]byte{
				0x00, 0x00, 0x01, 0x09, // invalid length: 265
				0x04, 0xD2, 0x16, 0x2E, // cancelRequestCode: 80877102
				0x00, 0x00, 0x22, 0xA0, // ProcessID: 8864
			}, make([]byte, 257)...), // 257 bytes secret key (1 byte too many)
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var msg CancelRequest
			err := msg.Decode(tt.src)

			if tt.expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expectedProcessID, msg.ProcessID)
			assert.Equal(t, tt.expectedSecretKey, msg.SecretKey)
		})
	}
}

func TestCancelRequestEncode(t *testing.T) {
	secretKey32 := []byte{
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10,
		0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
		0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F, 0x20,
	}

	tests := []struct {
		name        string
		msg         CancelRequest
		expected    []byte
		expectError bool
	}{
		{
			name: "Protocol 3.0 (4-byte key)",
			msg: CancelRequest{
				ProcessID: 8864,
				SecretKey: []byte{0xD9, 0x0C, 0xAE, 0xDB},
			},
			expected: []byte{
				0x00, 0x00, 0x00, 0x10, // length: 16
				0x04, 0xD2, 0x16, 0x2E, // cancelRequestCode: 80877102
				0x00, 0x00, 0x22, 0xA0, // ProcessID: 8864
				0xD9, 0x0C, 0xAE, 0xDB, // SecretKey
			},
		},
		{
			name: "Protocol 3.2 (32-byte key)",
			msg: CancelRequest{
				ProcessID: 8864,
				SecretKey: secretKey32,
			},
			// 4 byte length + 4 byte code + 4 byte ProcessID + 32 byte SecretKey = 44 bytes total
			expected: append([]byte{
				0x00, 0x00, 0x00, 0x2C, // length: 44 (12 + 32)
				0x04, 0xD2, 0x16, 0x2E, // cancelRequestCode: 80877102
				0x00, 0x00, 0x22, 0xA0, // ProcessID: 8864
			}, secretKey32...),
		},
		{
			name: "Too long secret key",
			msg: CancelRequest{
				ProcessID: 8864,
				SecretKey: make([]byte, 257), // 1 byte too many
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf, err := tt.msg.Encode(nil)
			if tt.expectError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expected, buf)
		})
	}
}
