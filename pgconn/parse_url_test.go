package pgconn

import (
	"maps"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// parseURLSettingsCorpus is a port of the libpq URI regression test corpus in
// src/interfaces/libpq/t/001_uri.pl (driven by test/libpq_uri_regress.c). Each
// case is either a success with the expected settings map or a failure with
// the expected error text.
//
// Cases marked pgxDiverges document deliberate differences from libpq; see the
// comment at the top of parse_url.go.
type parseURLSettingsCase struct {
	name         string
	connString   string
	wantSettings map[string]string
	wantErr      string // non-empty means the parse must fail with this exact error
	pgxDiverges  bool   // expected result differs from libpq; excluded from oracle comparison
}

var parseURLSettingsCorpus = []parseURLSettingsCase{
	// Success cases from 001_uri.pl.
	{
		name:         "user password host port db",
		connString:   "postgresql://uri-user:secret@host:12345/db",
		wantSettings: map[string]string{"user": "uri-user", "password": "secret", "host": "host", "port": "12345", "database": "db"},
	},
	{
		name:         "user host port db",
		connString:   "postgresql://uri-user@host:12345/db",
		wantSettings: map[string]string{"user": "uri-user", "host": "host", "port": "12345", "database": "db"},
	},
	{
		name:         "user host db",
		connString:   "postgresql://uri-user@host/db",
		wantSettings: map[string]string{"user": "uri-user", "host": "host", "database": "db"},
	},
	{
		name:         "host port db",
		connString:   "postgresql://host:12345/db",
		wantSettings: map[string]string{"host": "host", "port": "12345", "database": "db"},
	},
	{
		name:         "host db",
		connString:   "postgresql://host/db",
		wantSettings: map[string]string{"host": "host", "database": "db"},
	},
	{
		name:         "user host port empty path",
		connString:   "postgresql://uri-user@host:12345/",
		wantSettings: map[string]string{"user": "uri-user", "host": "host", "port": "12345"},
	},
	{
		name:         "user host empty path",
		connString:   "postgresql://uri-user@host/",
		wantSettings: map[string]string{"user": "uri-user", "host": "host"},
	},
	{
		name:         "user only",
		connString:   "postgresql://uri-user@",
		wantSettings: map[string]string{"user": "uri-user"},
	},
	{
		name:         "host port empty path",
		connString:   "postgresql://host:12345/",
		wantSettings: map[string]string{"host": "host", "port": "12345"},
	},
	{
		name:         "host port",
		connString:   "postgresql://host:12345",
		wantSettings: map[string]string{"host": "host", "port": "12345"},
	},
	{
		name:         "host empty path",
		connString:   "postgresql://host/",
		wantSettings: map[string]string{"host": "host"},
	},
	{
		name:         "host only",
		connString:   "postgresql://host",
		wantSettings: map[string]string{"host": "host"},
	},
	{
		name:         "empty",
		connString:   "postgresql://",
		wantSettings: map[string]string{},
	},
	{
		name:         "hostaddr only",
		connString:   "postgresql://?hostaddr=127.0.0.1",
		wantSettings: map[string]string{"hostaddr": "127.0.0.1"},
	},
	{
		name:         "host and hostaddr",
		connString:   "postgresql://example.com?hostaddr=63.1.2.4",
		wantSettings: map[string]string{"host": "example.com", "hostaddr": "63.1.2.4"},
	},
	{
		name:         "percent-encoded host",
		connString:   "postgresql://%68ost/",
		wantSettings: map[string]string{"host": "host"},
	},
	{
		name:         "query user",
		connString:   "postgresql://host/db?user=uri-user",
		wantSettings: map[string]string{"host": "host", "database": "db", "user": "uri-user"},
	},
	{
		name:         "query user and port",
		connString:   "postgresql://host/db?user=uri-user&port=12345",
		wantSettings: map[string]string{"host": "host", "database": "db", "user": "uri-user", "port": "12345"},
	},
	{
		name:         "percent-encoded query key",
		connString:   "postgresql://host/db?u%73er=someotheruser&port=12345",
		wantSettings: map[string]string{"host": "host", "database": "db", "user": "someotheruser", "port": "12345"},
	},
	{
		name:         "authority port and query user",
		connString:   "postgresql://host:12345?user=uri-user",
		wantSettings: map[string]string{"host": "host", "port": "12345", "user": "uri-user"},
	},
	{
		name:         "host and query user",
		connString:   "postgresql://host?user=uri-user",
		wantSettings: map[string]string{"host": "host", "user": "uri-user"},
	},
	{
		name:         "empty query",
		connString:   "postgresql://host?",
		wantSettings: map[string]string{"host": "host"},
	},
	{
		name:         "ipv6 port db",
		connString:   "postgresql://[::1]:12345/db",
		wantSettings: map[string]string{"host": "::1", "port": "12345", "database": "db"},
	},
	{
		name:         "ipv6 db",
		connString:   "postgresql://[::1]/db",
		wantSettings: map[string]string{"host": "::1", "database": "db"},
	},
	{
		name:         "ipv6 full",
		connString:   "postgresql://[2001:db8::1234]/",
		wantSettings: map[string]string{"host": "2001:db8::1234"},
	},
	{
		name:         "ipv6 garbage brackets accepted unvalidated",
		connString:   "postgresql://[200z:db8::1234]/",
		wantSettings: map[string]string{"host": "200z:db8::1234"},
	},
	{
		name:         "ipv6 bare",
		connString:   "postgresql://[::1]",
		wantSettings: map[string]string{"host": "::1"},
	},
	{
		name:         "short scheme empty",
		connString:   "postgres://",
		wantSettings: map[string]string{},
	},
	{
		name:         "short scheme empty path",
		connString:   "postgres:///",
		wantSettings: map[string]string{},
	},
	{
		name:         "short scheme db only",
		connString:   "postgres:///db",
		wantSettings: map[string]string{"database": "db"},
	},
	{
		name:         "short scheme user and db",
		connString:   "postgres://uri-user@/db",
		wantSettings: map[string]string{"user": "uri-user", "database": "db"},
	},
	{
		name:         "socket dir via query",
		connString:   "postgres://?host=/path/to/socket/dir",
		wantSettings: map[string]string{"host": "/path/to/socket/dir"},
	},
	{
		name:         "empty userinfo",
		connString:   "postgres://@host",
		wantSettings: map[string]string{"host": "host"},
	},
	{
		name:         "empty port",
		connString:   "postgres://host:/",
		wantSettings: map[string]string{"host": "host"},
	},
	{
		name:         "port without host",
		connString:   "postgres://:12345/",
		wantSettings: map[string]string{"port": "12345"},
	},
	{
		name:         "user and socket dir query",
		connString:   "postgres://otheruser@?host=/no/such/directory",
		wantSettings: map[string]string{"user": "otheruser", "host": "/no/such/directory"},
	},
	{
		name:         "user empty path and socket dir query",
		connString:   "postgres://otheruser@/?host=/no/such/directory",
		wantSettings: map[string]string{"user": "otheruser", "host": "/no/such/directory"},
	},
	{
		name:         "user port and socket path query",
		connString:   "postgres://otheruser@:12345?host=/no/such/socket/path",
		wantSettings: map[string]string{"user": "otheruser", "port": "12345", "host": "/no/such/socket/path"},
	},
	{
		name:         "user port db and socket path query",
		connString:   "postgres://otheruser@:12345/db?host=/path/to/socket",
		wantSettings: map[string]string{"user": "otheruser", "port": "12345", "database": "db", "host": "/path/to/socket"},
	},
	{
		name:         "port db and socket path query",
		connString:   "postgres://:12345/db?host=/path/to/socket",
		wantSettings: map[string]string{"port": "12345", "database": "db", "host": "/path/to/socket"},
	},
	{
		name:         "port and socket path query",
		connString:   "postgres://:12345?host=/path/to/socket",
		wantSettings: map[string]string{"port": "12345", "host": "/path/to/socket"},
	},
	{
		name:         "percent-encoded socket dir in authority",
		connString:   "postgres://%2Fvar%2Flib%2Fpostgresql/dbname",
		wantSettings: map[string]string{"host": "/var/lib/postgresql", "database": "dbname"},
	},
	{
		name:         "leading and trailing spaces in query params",
		connString:   "postgresql://host?  user = uri-user & port  = 12345 ",
		wantSettings: map[string]string{"host": "host", "user": "uri-user", "port": "12345"},
	},
	{
		name:         "sslmode disable",
		connString:   "postgresql://host?sslmode=disable",
		wantSettings: map[string]string{"host": "host", "sslmode": "disable"},
	},
	{
		name:         "sslmode prefer",
		connString:   "postgresql://host?sslmode=prefer",
		wantSettings: map[string]string{"host": "host", "sslmode": "prefer"},
	},
	{
		name:         "sslmode verify-full",
		connString:   "postgresql://host?sslmode=verify-full",
		wantSettings: map[string]string{"host": "host", "sslmode": "verify-full"},
	},

	// libpq rejects unknown query parameters; pgx accepts them so they can
	// become runtime parameters or pgx-specific options.
	{
		name:         "unknown query param accepted",
		connString:   "postgresql://host/db?u%7aer=someotheruser&port=12345",
		wantSettings: map[string]string{"host": "host", "database": "db", "uzer": "someotheruser", "port": "12345"},
		pgxDiverges:  true,
	},
	{
		name:         "unknown query param with empty value accepted",
		connString:   "postgresql://host?uzer=",
		wantSettings: map[string]string{"host": "host", "uzer": ""},
		pgxDiverges:  true,
	},

	// Failure cases from 001_uri.pl.
	{
		name:       "unterminated ipv6 bracket",
		connString: "postgres://[::1",
		wantErr:    `end of string reached when looking for matching "]" in IPv6 host address in URI`,
	},
	{
		name:       "empty ipv6 brackets",
		connString: "postgres://[]",
		wantErr:    "IPv6 host address may not be empty in URI",
	},
	{
		name:       "trailing garbage after ipv6 bracket",
		connString: "postgres://[::1]z",
		wantErr:    `unexpected character "z" at position 17 in URI (expected ":" or "/")`,
	},
	{
		name:       "query param without separator",
		connString: "postgresql://host?zzz",
		wantErr:    `missing key/value separator "=" in URI query parameter: "zzz"`,
	},
	{
		name:       "query params without separators",
		connString: "postgresql://host?value1&value2",
		wantErr:    `missing key/value separator "=" in URI query parameter: "value1"`,
	},
	{
		name:       "query param with extra separator",
		connString: "postgresql://host?key=key=value",
		wantErr:    `extra key/value separator "=" in URI query parameter: "key"`,
	},
	{
		name:       "invalid percent encoding in query value",
		connString: "postgres://host?dbname=%XXfoo",
		wantErr:    `invalid percent-encoded token: "%XXfoo"`,
	},
	{
		name:       "percent-encoded nul in host",
		connString: "postgresql://a%00b",
		wantErr:    `forbidden value %00 in percent-encoded value: "a%00b"`,
	},
	{
		name:       "invalid hex in percent encoding",
		connString: "postgresql://%zz",
		wantErr:    `invalid percent-encoded token: "%zz"`,
	},
	{
		name:       "truncated percent encoding",
		connString: "postgresql://%1",
		wantErr:    `invalid percent-encoded token: "%1"`,
	},
	{
		name:       "bare percent",
		connString: "postgresql://%",
		wantErr:    `invalid percent-encoded token: "%"`,
	},
	{
		name:       "interior space in query key",
		connString: "postgresql://host?  user user  =  uri  & port = 12345 12 ",
		wantErr:    `unexpected spaces found in "  user user  ", use percent-encoded spaces (%20) instead`,
	},
	{
		name:       "interior space in query value",
		connString: "postgresql://host?  user  =  uri-user  & port = 12345 12 ",
		wantErr:    `unexpected spaces found in " 12345 12 ", use percent-encoded spaces (%20) instead`,
	},
}

// parseURLSettingsMultiHostCorpus covers libpq's multiple-host extension. The
// upstream regression corpus has no multi-host URIs at all, so these cases are
// pgx's own, with expected values derived from libpq's parser behavior
// (conninfo_uri_parse_options joins hosts and ports into positionally aligned
// comma-separated lists; empty elements mean "use the default").
var parseURLSettingsMultiHostCorpus = []parseURLSettingsCase{
	{
		name:         "multi host no ports",
		connString:   "postgres://h1,h2/db",
		wantSettings: map[string]string{"host": "h1,h2", "port": ",", "database": "db"},
	},
	{
		name:         "multi host second port only",
		connString:   "postgres://h1,h2:5433/db",
		wantSettings: map[string]string{"host": "h1,h2", "port": ",5433", "database": "db"},
	},
	{
		name:         "multi host first port only",
		connString:   "postgres://h1:5433,h2/db",
		wantSettings: map[string]string{"host": "h1,h2", "port": "5433,", "database": "db"},
	},
	{
		name:         "multi host all ports",
		connString:   "postgres://h1:1,h2:2,h3:3/db",
		wantSettings: map[string]string{"host": "h1,h2,h3", "port": "1,2,3", "database": "db"},
	},
	{
		name:         "percent-encoded comma splits hosts",
		connString:   "postgres://a%2Cb/",
		wantSettings: map[string]string{"host": "a,b"},
	},
	{
		name:         "empty middle host",
		connString:   "postgres://h1,,h2/db",
		wantSettings: map[string]string{"host": "h1,,h2", "port": ",,", "database": "db"},
	},
	{
		name:         "empty leading host",
		connString:   "postgres://,h2/",
		wantSettings: map[string]string{"host": ",h2", "port": ","},
	},
	{
		name:         "trailing comma",
		connString:   "postgres://h1,/db",
		wantSettings: map[string]string{"host": "h1,", "port": ",", "database": "db"},
	},
	{
		name:         "multi host ipv6",
		connString:   "postgres://[::1]:5432,[::2]/db",
		wantSettings: map[string]string{"host": "::1,::2", "port": "5432,", "database": "db"},
	},
	{
		name:         "query port overrides authority ports",
		connString:   "postgres://h1:1,h2:2/db?port=9",
		wantSettings: map[string]string{"host": "h1,h2", "port": "9", "database": "db"},
	},
	{
		name:       "percent-encoded comma in port creates mismatched slot",
		connString: "postgres://h1:%2C/",
		// Stored faithfully; the count mismatch is detected when the list is
		// split in ParseConfigWithOptions, as it is in libpq at connect time.
		wantSettings: map[string]string{"host": "h1", "port": ","},
	},
}

func TestParseURLSettings(t *testing.T) {
	t.Parallel()

	corpus := append(append([]parseURLSettingsCase{}, parseURLSettingsCorpus...), parseURLSettingsMultiHostCorpus...)
	for _, tt := range corpus {
		t.Run(tt.name, func(t *testing.T) {
			settings, _, err := parseURLSettings(tt.connString)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantSettings, settings)
		})
	}
}

func TestParseURLSettingsImpliedEmptyPortList(t *testing.T) {
	t.Parallel()

	// The implied-empty-port-list flag must be true only for an all-empty port
	// list synthesized from the host list. Any user-supplied port text -- a
	// real port, an explicit ?port= query parameter even with an empty value,
	// or literal spaces that merely decode to empty -- is not implied:
	// ParseConfigWithOptions uses this flag to decide whether the "port" key
	// is exempt from ConnStringAllowedKeys.
	tests := []struct {
		connString  string
		wantImplied bool
	}{
		{"postgres://h1,h2/db", true},
		{"postgres://h1:,h2/db", true},
		{"postgres://h1,h2:5433/db", false},
		{"postgres://h/db", false}, // no port entry stored at all
		{"postgres://h1,h2/db?port=", false},
		{"postgres://h1,h2/db?port=5433", false},
		{"postgres://h: /db", false},     // space decodes to empty but is supplied port text
		{"postgres://h1: ,h2/db", false}, // likewise in one element of a list
	}
	for _, tt := range tests {
		settings, meta, err := parseURLSettings(tt.connString)
		require.NoError(t, err, tt.connString)
		assert.Equal(t, tt.wantImplied, meta.impliedEmptyPortList, tt.connString)
		if tt.wantImplied {
			assert.Equal(t, "", strings.Trim(settings["port"], ","), tt.connString)
		}
	}
}

func TestParseURLSettingsRepeatedSSL(t *testing.T) {
	t.Parallel()

	// ssl=true is rewritten to sslmode=require (JDBC alias, as libpq). Repeated
	// raw ssl values are resolved before that rewrite. If the final value is
	// "true", its position relative to the final explicit sslmode determines
	// which wins. If it is not "true", it remains under ssl and any independent
	// explicit sslmode is preserved.
	tests := []struct {
		connString   string
		wantSettings map[string]string
	}{
		{"postgres://h/db?ssl=true", map[string]string{"sslmode": "require"}},
		{"postgres://h/db?ssl=false&ssl=true", map[string]string{"sslmode": "require"}},
		{"postgres://h/db?ssl=true&ssl=false", map[string]string{"ssl": "false"}},
		{"postgres://h/db?sslmode=verify-full&ssl=true", map[string]string{"sslmode": "require"}},
		{"postgres://h/db?ssl=true&sslmode=disable", map[string]string{"sslmode": "disable"}},
		{"postgres://h/db?ssl=true&sslmode=disable&ssl=false", map[string]string{"sslmode": "disable", "ssl": "false"}},
		{"postgres://h/db?sslmode=verify-full&ssl=true&ssl=false", map[string]string{"sslmode": "verify-full", "ssl": "false"}},
	}
	for _, tt := range tests {
		want := map[string]string{"host": "h", "database": "db"}
		maps.Copy(want, tt.wantSettings)
		settings, _, err := parseURLSettings(tt.connString)
		require.NoError(t, err, tt.connString)
		assert.Equal(t, want, settings, tt.connString)
	}
}

func TestParseURLSettingsRejectsNulByte(t *testing.T) {
	t.Parallel()

	// A literal NUL would pass through percent-decoding into RuntimeParams and
	// then act as a delimiter inside the NUL-delimited startup message,
	// injecting extra parameters. %00 is already rejected; raw NUL bytes must
	// be too (net/url also rejected them).
	tests := []string{
		"postgres://host/db?application_name=foo\x00options=bar",
		"postgres://ho\x00st/db",
		"postgres://user\x00:pw@host/db",
		"postgres://host/d\x00b",
	}
	for _, connString := range tests {
		_, _, err := parseURLSettings(connString)
		require.EqualError(t, err, "forbidden NUL byte in connection string", "%q", connString)
	}
}

func TestParseURLSettingsSecretsNotInErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		connString string
		secret     string
		wantErr    string
	}{
		{
			name:       "invalid percent encoding in password",
			connString: "postgres://user:sec%zzret@host/db",
			secret:     "sec%zzret",
			wantErr:    "invalid percent-encoded token in password",
		},
		{
			name:       "percent-encoded nul in password",
			connString: "postgres://user:sec%00ret@host/db",
			secret:     "sec%00ret",
			wantErr:    "forbidden value %00 in percent-encoded value in password",
		},
		{
			name:       "interior space in password",
			connString: "postgres://user:sec ret@host/db",
			secret:     "sec ret",
			wantErr:    "unexpected spaces found in password, use percent-encoded spaces (%20) instead",
		},
		{
			name:       "invalid percent encoding in password query param",
			connString: "postgres://host/db?password=sec%zzret",
			secret:     "sec%zzret",
			wantErr:    "invalid percent-encoded token in password",
		},
		{
			name:       "invalid percent encoding in sslpassword query param",
			connString: "postgres://host/db?sslpassword=sec%zzret",
			secret:     "sec%zzret",
			wantErr:    "invalid percent-encoded token in sslpassword",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := parseURLSettings(tt.connString)
			require.EqualError(t, err, tt.wantErr)
			require.NotContains(t, err.Error(), tt.secret)
		})
	}
}

func TestParseURLSettingsUserinfoRules(t *testing.T) {
	t.Parallel()

	// The userinfo terminator is the first '@' that appears before any '/',
	// exactly as in libpq. The user may not contain a raw ':' or '@'; the
	// password may contain a raw ':' but not a raw '@'.
	tests := []struct {
		name         string
		connString   string
		wantSettings map[string]string
	}{
		{
			name:         "first at wins",
			connString:   "postgres://a@b@host/",
			wantSettings: map[string]string{"user": "a", "host": "b@host"},
		},
		{
			name:         "password may contain colon",
			connString:   "postgres://user:pa:ss@host/",
			wantSettings: map[string]string{"user": "user", "password": "pa:ss", "host": "host"},
		},
		{
			name:         "percent-encoded at in user",
			connString:   "postgres://us%40er@host/",
			wantSettings: map[string]string{"user": "us@er", "host": "host"},
		},
		{
			name:         "at after slash is not userinfo",
			connString:   "postgres://host/db@x",
			wantSettings: map[string]string{"host": "host", "database": "db@x"},
		},
		{
			name:         "empty user with password",
			connString:   "postgres://:secret@host/",
			wantSettings: map[string]string{"password": "secret", "host": "host"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			settings, _, err := parseURLSettings(tt.connString)
			require.NoError(t, err)
			assert.Equal(t, tt.wantSettings, settings)
		})
	}
}

func TestParseURLSettingsDataBytes(t *testing.T) {
	t.Parallel()

	// Bytes that RFC 3986 parsers treat as structure are plain data to libpq.
	tests := []struct {
		name         string
		connString   string
		wantSettings map[string]string
	}{
		{
			name:         "hash is data not fragment",
			connString:   "postgres://host/db?application_name=a#b",
			wantSettings: map[string]string{"host": "host", "database": "db", "application_name": "a#b"},
		},
		{
			name:         "plus is literal in query values",
			connString:   "postgres://host/db?options=-c+search_path%3Dfoo",
			wantSettings: map[string]string{"host": "host", "database": "db", "options": "-c+search_path=foo"},
		},
		{
			name:         "multi-segment path is dbname",
			connString:   "postgres://host/a/b",
			wantSettings: map[string]string{"host": "host", "database": "a/b"},
		},
		{
			name:         "repeated query param last wins",
			connString:   "postgres://host/db?application_name=first&application_name=second",
			wantSettings: map[string]string{"host": "host", "database": "db", "application_name": "second"},
		},
		{
			name:         "ssl true jdbc alias",
			connString:   "postgres://host/db?ssl=true",
			wantSettings: map[string]string{"host": "host", "database": "db", "sslmode": "require"},
		},
		{
			name:         "ssl false is not aliased",
			connString:   "postgres://host/db?ssl=false",
			wantSettings: map[string]string{"host": "host", "database": "db", "ssl": "false"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			settings, _, err := parseURLSettings(tt.connString)
			require.NoError(t, err)
			assert.Equal(t, tt.wantSettings, settings)
		})
	}
}

func TestURIDecode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		raw     string
		want    string
		wantErr string
	}{
		{raw: "abc", want: "abc"},
		{raw: "", want: ""},
		{raw: "%41", want: "A"},
		{raw: "%4a%4A", want: "JJ"},
		{raw: "%c3%a9", want: "\xc3\xa9"}, // raw bytes, no UTF-8 validation
		{raw: "a+b", want: "a+b"},         // '+' is not a space
		{raw: "%2520", want: "%20"},
		{raw: "  abc  ", want: "abc"},
		{raw: "   ", want: ""},
		{raw: "%20", want: " "}, // encoded space is data, not trimmed
		{raw: "a b", wantErr: `unexpected spaces found in "a b", use percent-encoded spaces (%20) instead`},
		{raw: "%", wantErr: `invalid percent-encoded token: "%"`},
		{raw: "%1", wantErr: `invalid percent-encoded token: "%1"`},
		{raw: "%zz", wantErr: `invalid percent-encoded token: "%zz"`},
		{raw: "%4 ", wantErr: `invalid percent-encoded token: "%4 "`},
		{raw: "a%00b", wantErr: `forbidden value %00 in percent-encoded value: "a%00b"`},
	}

	for _, tt := range tests {
		t.Run(tt.raw, func(t *testing.T) {
			got, err := uriDecode(tt.raw, "")
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// FuzzParseURLSettings checks that parseURLSettings never panics, and -- when
// PGX_TEST_LIBPQ_URI_REGRESS points at a built libpq_uri_regress binary --
// differentially compares every input against libpq itself.
//
// To build the oracle from the PostgreSQL source checkout in
// references/postgres (provisioned with `rake references:setup`):
//
//	cd references/postgres
//	./configure --without-readline --without-icu --without-zlib
//	make -C src/interfaces/libpq
//	make -C src/interfaces/libpq/test libpq_uri_regress
//	cd -
//	PGX_TEST_LIBPQ_URI_REGRESS=references/postgres/src/interfaces/libpq/test/libpq_uri_regress \
//	  go test -run FuzzParseURLSettings -fuzz FuzzParseURLSettings -fuzztime 60s ./pgconn/
func FuzzParseURLSettings(f *testing.F) {
	for _, tt := range parseURLSettingsCorpus {
		f.Add(tt.connString)
	}
	for _, tt := range parseURLSettingsMultiHostCorpus {
		f.Add(tt.connString)
	}

	oracle := newURIRegressOracle(f)

	f.Fuzz(func(t *testing.T, connString string) {
		if !strings.HasPrefix(connString, "postgres://") && !strings.HasPrefix(connString, "postgresql://") {
			t.Skip("not a connection URI")
		}
		settings, _, err := parseURLSettings(connString)
		if err == nil && settings == nil {
			t.Fatal("nil settings without error")
		}
		if oracle != nil {
			oracle.compare(t, connString, settings, err)
		}
	})
}

// TestParseURLSettingsOracle runs the full corpus against libpq itself. It is
// skipped unless PGX_TEST_LIBPQ_URI_REGRESS is set; see FuzzParseURLSettings
// for build instructions.
func TestParseURLSettingsOracle(t *testing.T) {
	oracle := newURIRegressOracle(t)
	if oracle == nil {
		t.Skip("PGX_TEST_LIBPQ_URI_REGRESS not set")
	}

	corpus := append(append([]parseURLSettingsCase{}, parseURLSettingsCorpus...), parseURLSettingsMultiHostCorpus...)
	for _, tt := range corpus {
		if tt.pgxDiverges {
			continue
		}
		t.Run(tt.name, func(t *testing.T) {
			settings, _, err := parseURLSettings(tt.connString)
			oracle.compare(t, tt.connString, settings, err)
		})
	}
}

type uriRegressOracle struct {
	binPath string
}

func newURIRegressOracle(t testing.TB) *uriRegressOracle {
	binPath := os.Getenv("PGX_TEST_LIBPQ_URI_REGRESS")
	if binPath == "" {
		return nil
	}
	if _, err := os.Stat(binPath); err != nil {
		t.Fatalf("PGX_TEST_LIBPQ_URI_REGRESS: %v", err)
	}
	return &uriRegressOracle{binPath: binPath}
}

var uriRegressOptionRegexp = regexp.MustCompile(`([a-z_]+)='([^']*)'`)

// compare runs connString through libpq_uri_regress and compares libpq's
// verdict and core option values against ours. Differences covered by pgx's
// documented extensions are tolerated.
func (o *uriRegressOracle) compare(t *testing.T, connString string, settings map[string]string, parseErr error) {
	if strings.ContainsAny(connString, "\x00\n") {
		return // cannot be passed through an argv / line-oriented tool
	}

	cmd := exec.Command(o.binPath, connString)
	// Scrub PG* variables so PQconndefaults is stable, as 001_uri.pl does.
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "LD_LIBRARY_PATH=" + os.Getenv("LD_LIBRARY_PATH")}
	out, runErr := cmd.CombinedOutput()

	if runErr != nil {
		// libpq rejected the URI. pgx must reject it too, unless the failure
		// is libpq's unknown-parameter check, which pgx deliberately skips.
		if strings.Contains(string(out), "invalid URI query parameter") {
			return
		}
		if parseErr == nil {
			t.Errorf("libpq rejected %q (%s) but pgx accepted with settings %v", connString, strings.TrimSpace(string(out)), settings)
		}
		return
	}
	if parseErr != nil {
		t.Errorf("libpq accepted %q but pgx rejected: %v", connString, parseErr)
		return
	}

	// libpq_uri_regress prints values with no quote escaping, so output
	// containing a single quote inside a value cannot be parsed reliably.
	for _, v := range settings {
		if strings.Contains(v, "'") {
			return
		}
	}

	// libpq_uri_regress prints only options that differ from PQconndefaults,
	// as keyword='value' pairs. Every option it prints must match ours; any
	// core option we set that it did not print must equal the libpq default.
	libpqDefaults := map[string]string{"port": "5432"}
	keyMap := map[string]string{"user": "user", "password": "password", "dbname": "database", "host": "host", "hostaddr": "hostaddr", "port": "port"}

	printed := map[string]string{}
	for _, m := range uriRegressOptionRegexp.FindAllStringSubmatch(string(out), -1) {
		if ours, ok := keyMap[m[1]]; ok {
			printed[ours] = m[2]
		}
	}

	for key, libpqVal := range printed {
		if ourVal, ok := settings[key]; !ok || ourVal != libpqVal {
			t.Errorf("parse %q: libpq has %s=%q, pgx has %q (present=%v)", connString, key, libpqVal, ourVal, ok)
		}
	}
	for key, ourVal := range settings {
		coreKey := false
		for _, v := range keyMap {
			if key == v {
				coreKey = true
			}
		}
		if !coreKey {
			continue // sslmode etc. are default-filtered unpredictably; skip
		}
		if _, ok := printed[key]; !ok && ourVal != libpqDefaults[key] {
			t.Errorf("parse %q: pgx has %s=%q but libpq printed no value (default %q)", connString, key, ourVal, libpqDefaults[key])
		}
	}
}
