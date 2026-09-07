package pgproto3_test

import (
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

func TestStartupMessageEncodeRejectsNulByte(t *testing.T) {
	t.Parallel()

	// The startup message body is a flat run of NUL-delimited strings with no
	// per-field length prefixes, so a NUL in a parameter name or value injects
	// additional parameters rather than truncating the field.
	tests := []struct {
		name    string
		params  map[string]string
		wantErr string
	}{
		{
			name:    "nul in value hijacks user",
			params:  map[string]string{"user": "lowpriv", "application_name": "x\x00user\x00admin"},
			wantErr: `startup message parameter "application_name" contains NUL byte in value`,
		},
		{
			name:    "nul in database",
			params:  map[string]string{"user": "u", "database": "d\x00options\x00-c search_path=evil"},
			wantErr: `startup message parameter "database" contains NUL byte in value`,
		},
		{
			name:    "nul in parameter name",
			params:  map[string]string{"user": "u", "application_name\x00user\x00admin": "x"},
			wantErr: "startup message parameter name contains NUL byte",
		},
		{
			name:    "trailing nul in value",
			params:  map[string]string{"user": "u\x00"},
			wantErr: `startup message parameter "user" contains NUL byte in value`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &pgproto3.StartupMessage{
				ProtocolVersion: pgproto3.ProtocolVersion30,
				Parameters:      tt.params,
			}
			_, err := msg.Encode(nil)
			require.EqualError(t, err, tt.wantErr)
		})
	}
}

func TestStartupMessageEncodeNulErrorDoesNotLeakValue(t *testing.T) {
	t.Parallel()

	msg := &pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersion30,
		Parameters:      map[string]string{"options": "-c password_encryption=hunter2\x00user\x00admin"},
	}
	_, err := msg.Encode(nil)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "hunter2")
}

func TestStartupMessageEncodeAllowsOrdinaryParameters(t *testing.T) {
	t.Parallel()

	msg := &pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersion30,
		Parameters:      map[string]string{"user": "jack", "database": "pgx_test", "application_name": "pgx"},
	}
	buf, err := msg.Encode(nil)
	require.NoError(t, err)

	var decoded pgproto3.StartupMessage
	require.NoError(t, decoded.Decode(buf[4:]))
	require.Equal(t, msg.Parameters, decoded.Parameters)
}
