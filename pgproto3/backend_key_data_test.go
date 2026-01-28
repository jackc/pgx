package pgproto3

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackendKeyDataDecodeProtocol30(t *testing.T) {
	// Protocol 3.0: 8 bytes (4 for ProcessID, 4 for SecretKey)
	src := []byte{
		0x00, 0x00, 0x22, 0xA0, // ProcessID: 8864
		0xD9, 0x0C, 0xAE, 0xDB, // SecretKey
	}

	var msg BackendKeyData
	err := msg.Decode(src)
	require.NoError(t, err)
	assert.Equal(t, uint32(8864), msg.ProcessID)
	expectedKey := []byte{0xD9, 0x0C, 0xAE, 0xDB}
	assert.Equal(t, expectedKey, msg.SecretKey)
}

func TestBackendKeyDataDecodeProtocol32(t *testing.T) {
	// Protocol 3.2: variable-length key (using 32 bytes here)
	secretKey := []byte{
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10,
		0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
		0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F, 0x20,
	}

	src := append([]byte{0x00, 0x00, 0x22, 0xA0}, secretKey...) // ProcessID: 8864

	var msg BackendKeyData
	err := msg.Decode(src)
	require.NoError(t, err)

	assert.Equal(t, uint32(8864), msg.ProcessID)
	assert.Equal(t, secretKey, msg.SecretKey)
}

func TestBackendKeyDataEncodeProtocol30(t *testing.T) {
	msg := BackendKeyData{
		ProcessID: 8864,
		SecretKey: []byte{0xD9, 0x0C, 0xAE, 0xDB},
	}

	buf, err := msg.Encode(nil)
	require.NoError(t, err)

	expected := []byte{
		'K',                    // message type
		0x00, 0x00, 0x00, 0x0C, // length: 12 (4 + 8)
		0x00, 0x00, 0x22, 0xA0, // ProcessID: 8864
		0xD9, 0x0C, 0xAE, 0xDB, // SecretKey
	}

	assert.Equal(t, expected, buf)
}

func TestBackendKeyDataEncodeProtocol32(t *testing.T) {
	secretKey := []byte{
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10,
		0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
		0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F, 0x20,
	}

	msg := BackendKeyData{
		ProcessID: 8864,
		SecretKey: secretKey,
	}

	buf, err := msg.Encode(nil)
	require.NoError(t, err)

	// 'K' + 4 byte length + 4 byte ProcessID + 32 byte SecretKey = 41 bytes total, length field is 40
	expected := append([]byte{
		'K',                    // message type
		0x00, 0x00, 0x00, 0x28, // length: 40 (4 + 4 + 32)
		0x00, 0x00, 0x22, 0xA0, // ProcessID: 8864
	}, secretKey...)

	assert.Equal(t, expected, buf)
}
