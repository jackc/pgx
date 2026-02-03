package pgproto3

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNegotiateProtocolVersionDecode(t *testing.T) {
	src := []byte{
		0x00, 0x00, 0x00, 0x00, // NewestMinorProtocol: 0
		0x00, 0x00, 0x00, 0x02, // Option count: 2
		'o', 'p', 't', '1', 0x00, // "opt1"
		'o', 'p', 't', '2', 0x00, // "opt2"
	}

	var msg NegotiateProtocolVersion
	err := msg.Decode(src)
	require.NoError(t, err)

	assert.Equal(t, uint32(0), msg.NewestMinorProtocol)
	assert.Equal(t, []string{"opt1", "opt2"}, msg.UnrecognizedOptions)
}

func TestNegotiateProtocolVersionDecodeNoOptions(t *testing.T) {
	// Message: minor version 2, no unrecognized options
	src := []byte{
		0x00, 0x00, 0x00, 0x02, // NewestMinorProtocol: 2
		0x00, 0x00, 0x00, 0x00, // Option count: 0
	}

	var msg NegotiateProtocolVersion
	err := msg.Decode(src)
	require.NoError(t, err)
	assert.Equal(t, uint32(2), msg.NewestMinorProtocol)
	assert.Equal(t, 0, len(msg.UnrecognizedOptions))
}

func TestNegotiateProtocolVersionEncode(t *testing.T) {
	msg := NegotiateProtocolVersion{
		NewestMinorProtocol: 0,
		UnrecognizedOptions: []string{"opt1", "opt2"},
	}

	buf, err := msg.Encode(nil)
	require.NoError(t, err)

	expected := []byte{
		'v',                    // message type
		0x00, 0x00, 0x00, 0x16, // length: 22 (4 for length + 4 + 4 + 5 + 5)
		0x00, 0x00, 0x00, 0x00, // NewestMinorProtocol: 0
		0x00, 0x00, 0x00, 0x02, // Option count: 2
		'o', 'p', 't', '1', 0x00,
		'o', 'p', 't', '2', 0x00,
	}

	require.Equal(t, expected, buf)
}

func TestNegotiateProtocolVersionJSON(t *testing.T) {
	msg := NegotiateProtocolVersion{
		NewestMinorProtocol: 0,
		UnrecognizedOptions: []string{"opt1"},
	}

	data, err := json.Marshal(msg)
	require.NoError(t, err)

	var decoded NegotiateProtocolVersion
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, msg, decoded)
}

func TestJSONUnmarshalNegotiateProtocolVersion(t *testing.T) {
	data := []byte(`{"Type":"NegotiateProtocolVersion","NewestMinorProtocol":0,"UnrecognizedOptions":["opt1"]}`)
	want := NegotiateProtocolVersion{
		NewestMinorProtocol: 0,
		UnrecognizedOptions: []string{"opt1"},
	}

	var got NegotiateProtocolVersion
	err := json.Unmarshal(data, &got)
	require.NoError(t, err)
	assert.Equal(t, want, got)
}
