package pgconn_test

import (
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
			name:        "url with slash and query-like text in password",
			err:         pgconn.NewParseConfigError("postgres://user:pass/word?x=y@host/db", "msg", nil),
			expectedMsg: "cannot parse `postgres://user:xxxxxx@host/db`: msg",
		},
		{
			name:        "url with colon inside stranded password",
			err:         pgconn.NewParseConfigError("postgres://a@user:sec:ret@host/db", "msg", nil),
			expectedMsg: "cannot parse `postgres://a@user:xxxxxx@host/db`: msg",
		},
		{
			name:        "valid url with at sign in query value and query password",
			err:         pgconn.NewParseConfigError("postgres://host:5432/db?application_name=me@example.com&password=supersecret&sslmode=bogus", "msg", nil),
			expectedMsg: "cannot parse `postgres://host:5432/db?application_name=me@example.com&password=xxxxx&sslmode=bogus`: msg",
		},
		{
			name:        "valid url with userinfo password and at sign in query value",
			err:         pgconn.NewParseConfigError("postgres://user:pw@host:5432/db?options=x@y&password=supersecret", "msg", nil),
			expectedMsg: "cannot parse `postgres://user:xxxxx@host:5432/db?options=x@y&password=xxxxx`: msg",
		},
		{
			name:        "valid url with at sign in dbname",
			err:         pgconn.NewParseConfigError("postgres://host/db@name?password=supersecret", "msg", nil),
			expectedMsg: "cannot parse `postgres://host/db@name?password=xxxxx`: msg",
		},
		{
			name:        "url with question mark inside ipv6 bracket",
			err:         pgconn.NewParseConfigError("postgres://[foo?bar]/db?password=supersecret", "msg", nil),
			expectedMsg: "cannot parse `postgres://[foo?bar]/db?password=xxxxx`: msg",
		},
		{
			name:        "url with percent-encoded password key",
			err:         pgconn.NewParseConfigError("postgres://host/db?pass%77ord=supersecret", "msg", nil),
			expectedMsg: "cannot parse `postgres://host/db?pass%77ord=xxxxx`: msg",
		},
		{
			name:        "url with space in query password",
			err:         pgconn.NewParseConfigError("postgres://host/db?password=super secret&sslmode=disable", "msg", nil),
			expectedMsg: "cannot parse `postgres://host/db?password=xxxxx&sslmode=disable`: msg",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.EqualError(t, tt.err, tt.expectedMsg)
		})
	}
}
