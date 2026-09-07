package pgconn

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

// parseKeywordValueSettingsCorpus seeds FuzzParseKeywordValueSettings. It
// covers the shape of the grammar -- bare and quoted values, backslash
// escapes, empty values, and the whitespace handling around each -- plus the
// inputs of past parser bugs so they stay in the corpus permanently.
var parseKeywordValueSettingsCorpus = []string{
	"",
	"host=localhost",
	"host=localhost user=jack",
	"host=localhost user=jack password=secret port=5432 dbname=mydb",
	"host='local host'",
	"host='' user=jack",
	"host= user= password= port= database=",
	"user=",
	"host=x  user=y",
	"dbname=foo ",
	"dbname=foo\t",
	"dbname='foo'  ",
	" host=x",
	"host = x",

	// Backslash escapes, quoted and unquoted.
	`host='a\\b'`,
	`host='a\'b'`,
	`password='p\'w'`,
	`host=a\\b`,
	`host=a\'b`,

	// Malformed inputs the parser must reject rather than mishandle.
	"host",
	"=",
	"0 0=",
	"us er=jack",
	"= user=jack",
	"'a'=b",
	`host='`,
	`host='a`,
	`\`,
	"host=x\x00",

	// https://github.com/jackc/pgconn/issues/49 -- unquoted trailing backslash.
	`x=x\`,
	`host=a\`,

	// Trailing backslash inside a quoted value panicked with a slice bounds
	// error before it was bounds-checked; keep every shape of it seeded.
	`='\`,
	`0='\`,
	`host='a\`,
	`host='a\\b\`,
}

// FuzzParseKeywordValueSettings checks that parseKeywordValueSettings never
// panics, and -- when PGX_TEST_LIBPQ_URI_REGRESS points at a built
// libpq_uri_regress binary -- differentially compares every input against
// libpq itself. libpq_uri_regress calls PQconninfoParse, which accepts
// keyword/value strings as well as URIs, so the same oracle serves both
// parsers. Inputs are partitioned between this target and
// FuzzParseURLSettings exactly as ParseConfigWithOptions dispatches them.
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
//	  go test -run FuzzParseKeywordValueSettings -fuzz FuzzParseKeywordValueSettings -fuzztime 60s ./pgconn/
func FuzzParseKeywordValueSettings(f *testing.F) {
	for _, connString := range parseKeywordValueSettingsCorpus {
		f.Add(connString)
	}

	oracle := newURIRegressOracle(f)

	f.Fuzz(func(t *testing.T, connString string) {
		if strings.HasPrefix(connString, "postgres://") || strings.HasPrefix(connString, "postgresql://") {
			t.Skip("dispatched to parseURLSettings; covered by FuzzParseURLSettings")
		}
		settings, err := parseKeywordValueSettings(connString)
		if err == nil && settings == nil {
			t.Fatal("nil settings without error")
		}
		if oracle != nil {
			oracle.compareKeywordValue(t, connString, settings, err)
		}
	})
}

// compareKeywordValue runs connString through libpq_uri_regress and compares
// libpq's verdict and core option values against ours. It is the
// keyword/value counterpart of compare; the tolerated differences are not the
// same, so the two are kept apart.
func (o *uriRegressOracle) compareKeywordValue(t *testing.T, connString string, settings map[string]string, parseErr error) {
	if strings.ContainsAny(connString, "\x00\n") {
		return // cannot be passed through an argv / line-oriented tool
	}

	cmd := exec.Command(o.binPath, connString)
	// Scrub PG* variables so PQconndefaults is stable, as 001_uri.pl does.
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "LD_LIBRARY_PATH=" + os.Getenv("LD_LIBRARY_PATH")}
	out, runErr := cmd.CombinedOutput()

	if runErr != nil {
		// libpq rejected the string. pgx must reject it too, unless the
		// failure is libpq's unknown-keyword check, which pgx deliberately
		// skips so that unknown keys reach ConnStringAllowedKeys and
		// RuntimeParams instead.
		if strings.Contains(string(out), "invalid connection option") {
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

	for _, v := range settings {
		// libpq_uri_regress prints values with no quote escaping, so output
		// containing a single quote inside a value cannot be parsed reliably.
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
		// pgx drops an empty user= so that the environment and the OS user
		// still apply; libpq keeps it as an empty string.
		if key == "user" && libpqVal == "" {
			continue
		}
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

// TestParseKeywordValueSettingsBackslash covers libpq's backslash rule: a
// backslash is dropped and whatever follows it is taken literally. pgx used to
// unescape only \\ and \', leaving every other backslash in the value, so a
// Windows path written without doubling came through intact. It no longer
// does, matching libpq: such a path has to be escaped or quoted-and-escaped,
// exactly as libpq requires.
func TestParseKeywordValueSettingsBackslash(t *testing.T) {
	tests := []struct {
		name       string
		connString string
		key        string
		want       string
	}{
		{name: "escaped backslash, quoted", connString: `host='a\\b'`, key: "host", want: `a\b`},
		{name: "escaped backslash, unquoted", connString: `host=a\\b`, key: "host", want: `a\b`},
		{name: "escaped quote, quoted", connString: `host='a\'b'`, key: "host", want: "a'b"},
		{name: "escaped quote, unquoted", connString: `host=a\'b`, key: "host", want: "a'b"},

		// A backslash before an ordinary character is dropped, so these are
		// the letter n and a literal space rather than an escape sequence.
		{name: "backslash before letter, quoted", connString: `host='a\nb'`, key: "host", want: "anb"},
		{name: "backslash before letter, unquoted", connString: `host=a\nb`, key: "host", want: "anb"},
		{name: "backslash before space", connString: `host=a\ b`, key: "host", want: "a b"},

		// A trailing backslash in an unquoted value escapes the end of the
		// string: libpq drops it and ends the value there. This used to be
		// rejected with "invalid backslash" (be69c1c1).
		{name: "trailing backslash", connString: `host=a\`, key: "host", want: "a"},
		{name: "trailing backslash, empty value", connString: `host=\`, key: "host", want: ""},
		{name: "trailing backslash after escaped pair", connString: `host=a\\b\`, key: "host", want: `a\b`},

		// Windows paths now need the same escaping libpq requires.
		{name: "windows path, escaped", connString: `sslcert=C:\\path\\to\\cert`, key: "sslcert", want: `C:\path\to\cert`},
		{name: "windows path, unescaped", connString: `sslcert=C:\path\to\cert`, key: "sslcert", want: "C:pathtocert"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			settings, err := parseKeywordValueSettings(tt.connString)
			if err != nil {
				t.Fatalf("parse %q: %v", tt.connString, err)
			}
			if got := settings[tt.key]; got != tt.want {
				t.Errorf("parse %q: %s = %q, want %q", tt.connString, tt.key, got, tt.want)
			}
		})
	}
}

// TestParseKeywordValueSettingsQuotedTrailingBackslash covers the one place a
// trailing backslash is still an error. Inside quotes it escapes the string
// terminator, which leaves the quoted string unterminated -- libpq fails the
// same way, with the same message.
func TestParseKeywordValueSettingsQuotedTrailingBackslash(t *testing.T) {
	for _, connString := range []string{`='\`, `0='\`, `host='a\`, `host='a\\b\`} {
		t.Run(connString, func(t *testing.T) {
			_, err := parseKeywordValueSettings(connString)
			if err == nil {
				t.Fatalf("parse %q: expected an error", connString)
			}
			if want := "unterminated quoted string in connection info string"; err.Error() != want {
				t.Errorf("parse %q: err = %q, want %q", connString, err, want)
			}
		})
	}
}

// TestParseKeywordValueSettingsKeywordSpace covers libpq's keyword scan: a
// keyword is a run of non-space characters and the next non-space character
// must be '=', so whitespace inside a keyword is an error while whitespace
// around the '=' is not. pgx used to trim the outer whitespace only and accept
// the space as part of the key, which turned a typo into a bogus RuntimeParam
// that only the server rejected.
func TestParseKeywordValueSettingsKeywordSpace(t *testing.T) {
	rejected := []struct {
		connString string
		wantErr    string
	}{
		{connString: "us er=jack", wantErr: `missing "=" after "us" in connection info string`},
		{connString: "0 0=", wantErr: `missing "=" after "0" in connection info string`},
		{connString: "a b c=d", wantErr: `missing "=" after "a" in connection info string`},
		{connString: "host=x us\ter=jack", wantErr: `missing "=" after "us" in connection info string`},
		// An unquoted value containing a space leaves the remainder to be read
		// as a keyword, which is where this most often shows up in practice.
		{connString: "application_name=my app host=x", wantErr: `missing "=" after "app" in connection info string`},
	}

	for _, tt := range rejected {
		t.Run(tt.connString, func(t *testing.T) {
			_, err := parseKeywordValueSettings(tt.connString)
			if err == nil {
				t.Fatalf("parse %q: expected an error", tt.connString)
			}
			if err.Error() != tt.wantErr {
				t.Errorf("parse %q: err = %q, want %q", tt.connString, err, tt.wantErr)
			}
		})
	}

	// Whitespace around '=' is not affected: libpq accepts these and so must we.
	for _, connString := range []string{"host = localhost", "host =localhost", "host= localhost", "  host\t=\tlocalhost  "} {
		t.Run(connString, func(t *testing.T) {
			settings, err := parseKeywordValueSettings(connString)
			if err != nil {
				t.Fatalf("parse %q: %v", connString, err)
			}
			if got, want := settings["host"], "localhost"; got != want {
				t.Errorf("parse %q: host = %q, want %q", connString, got, want)
			}
		})
	}
}
