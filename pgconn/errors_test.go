package pgconn_test

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
)

func TestConfigError(t *testing.T) {
	tests := []struct {
		name        string
		err         error
		expectedMsg string
	}{
		{
			name:        "url with password",
			err:         pgconn.NewParseConfigError("postgresql://foo:password@host", "msg", nil),
			expectedMsg: "cannot parse `postgresql://foo:xxxxx@host`: msg",
		},
		{
			name:        "keyword/value with password unquoted",
			err:         pgconn.NewParseConfigError("host=host password=password user=user", "msg", nil),
			expectedMsg: "cannot parse `host=host password=xxxxx user=user`: msg",
		},
		{
			name:        "keyword/value with password quoted",
			err:         pgconn.NewParseConfigError("host=host password='pass word' user=user", "msg", nil),
			expectedMsg: "cannot parse `host=host password=xxxxx user=user`: msg",
		},
		{
			name:        "weird url",
			err:         pgconn.NewParseConfigError("postgresql://foo::password@host:1:", "msg", nil),
			expectedMsg: "cannot parse `postgresql://foo:xxxxx@host:1:`: msg",
		},
		{
			name:        "weird url with slash in password",
			err:         pgconn.NewParseConfigError("postgres://user:pass/word@host:5432/db_name", "msg", nil),
			expectedMsg: "cannot parse `postgres://user:xxxxxx@host:5432/db_name`: msg",
		},
		{
			name:        "url without password",
			err:         pgconn.NewParseConfigError("postgresql://other@host/db", "msg", nil),
			expectedMsg: "cannot parse `postgresql://other@host/db`: msg",
		},
		{
			name:        "url with password, include error",
			err:         pgconn.NewParseConfigError("postgresql://foo:password@host", "msg", errors.New(`failed to parse as URL (postgresql://foo:password@host)`)),
			expectedMsg: "cannot parse `postgresql://foo:xxxxx@host`: msg (failed to parse as URL (postgresql://foo:xxxxxx@host))",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.EqualError(t, tt.err, tt.expectedMsg)
		})
	}
}
