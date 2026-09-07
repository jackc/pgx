package pgconn

import (
	"errors"
	"fmt"
	"maps"
	"strings"
)

// This file parses PostgreSQL connection URIs using a parser designed to
// exactly match libpq's URI parser behavior (conninfo_uri_parse_options,
// conninfo_uri_parse_params, and conninfo_uri_decode in
// src/interfaces/libpq/fe-connect.c), so that pgx accepts and rejects exactly
// the same URIs as libpq, including libpq's multiple-host extension
// (postgresql://host1:port1,host2:port2/db), which is not valid RFC 3986 syntax.
//
// Deliberate differences from libpq:
//
//   - Query parameters that libpq does not recognize are accepted. They become
//     runtime parameters or pgx-specific options (e.g. pool_max_conns) instead
//     of failing the parse.
//   - Error messages avoid quoting the unredacted connection string and mask
//     recognizable password fields on a best-effort basis. Invalid connection
//     strings can be structurally ambiguous, so password redaction cannot be
//     guaranteed for every malformed input. libpq quotes the failing input
//     verbatim, which can leak a password into error messages and logs.
//   - Literal NUL bytes are rejected. libpq never sees them because C strings
//     end at the first NUL; a Go string can carry one into the startup packet,
//     where it would act as a protocol delimiter and inject extra parameters.

// parseURLMeta reports provenance facts about a parsed URI that
// ParseConfigWithOptions needs for ConnStringAllowedKeys validation and that
// cannot be recovered from the settings map itself.
type parseURLMeta struct {
	// impliedEmptyPortList: the "port" entry is an all-empty list synthesized
	// from the host list (postgres://h1,h2 yields port=",") with no port
	// bytes supplied by the user. Determined from the raw port syntax, not
	// the decoded value: a port of literal spaces (postgres://h: /db)
	// decodes to empty but was user-supplied.
	impliedEmptyPortList bool
	// sawRawSSLKey / sawSSLTrueAlias / sawExplicitSSLModeKey record which
	// spellings of the ssl and sslmode query keys appeared anywhere in the
	// URI -- including occurrences later superseded under last-occurrence-
	// wins. The ssl=true alias rewrite and ordinary repeated-key handling can
	// remove those occurrences from the final settings map, so
	// ConnStringAllowedKeys cannot fail closed from that map alone; it must see
	// every key the user actually wrote.
	sawRawSSLKey          bool // key "ssl" with a value other than "true"
	sawSSLTrueAlias       bool // key "ssl" with value "true" (stored as sslmode=require)
	sawExplicitSSLModeKey bool // key "sslmode"
}

// parseURLSettings parses a connection URI into a settings map. connString
// must start with "postgres://" or "postgresql://".
//
// Multiple hosts are represented in the returned map the same way libpq
// represents them internally: settings["host"] and settings["port"] are
// comma-separated lists that are positionally aligned, with empty elements
// meaning "use the default". The lists are split in ParseConfigWithOptions.
func parseURLSettings(connString string) (settings map[string]string, meta parseURLMeta, err error) {
	settings = make(map[string]string)

	if strings.IndexByte(connString, 0) >= 0 {
		return nil, meta, errors.New("forbidden NUL byte in connection string")
	}

	p, ok := strings.CutPrefix(connString, "postgresql://")
	if !ok {
		p, ok = strings.CutPrefix(connString, "postgres://")
	}
	if !ok {
		return nil, meta, errors.New("invalid URI propagated to internal parser routine")
	}

	// Look ahead for a possible user credentials designator. Like libpq, only
	// a '/' stops the search, so a '@' anywhere before the path -- even inside
	// what looks like a query string -- is treated as the userinfo terminator.
	if i := strings.IndexAny(p, "@/"); i >= 0 && p[i] == '@' {
		user, password, hasPassword := strings.Cut(p[:i], ":")
		p = p[i+1:]
		if user != "" {
			val, err := uriDecode(user, "")
			if err != nil {
				return nil, meta, err
			}
			settings["user"] = val
		}
		if hasPassword && password != "" {
			val, err := uriDecode(password, "password")
			if err != nil {
				return nil, meta, err
			}
			settings["password"] = val
		}
	}

	// Parse the comma-separated list of netloc[:port] specifications. Hosts
	// and ports accumulate into positionally aligned comma-joined lists: every
	// separator appends a comma to both, so postgres://h1,h2:5433 yields
	// host="h1,h2" port=",5433".
	var hostBuf, portBuf strings.Builder

	for {
		if peekByte(p) == '[' {
			// IPv6 address. The bracket contents are not validated, matching libpq.
			end := strings.IndexByte(p, ']')
			if end < 0 {
				return nil, meta, errors.New(`end of string reached when looking for matching "]" in IPv6 host address in URI`)
			}
			if end == 1 {
				return nil, meta, errors.New("IPv6 host address may not be empty in URI")
			}
			hostBuf.WriteString(p[1:end])
			p = p[end+1:]
			if c := peekByte(p); c != 0 && c != ':' && c != '/' && c != '?' && c != ',' {
				return nil, meta, fmt.Errorf(`unexpected character "%c" at position %d in URI (expected ":" or "/")`, c, len(connString)-len(p)+1)
			}
		} else {
			// DNS-named or IPv4 netloc: everything up to a ':', '/', '?', or ','.
			i := strings.IndexAny(p, ":/?,")
			if i < 0 {
				i = len(p)
			}
			hostBuf.WriteString(p[:i])
			p = p[i:]
		}

		if peekByte(p) == ':' {
			p = p[1:]
			i := strings.IndexAny(p, "/?,")
			if i < 0 {
				i = len(p)
			}
			portBuf.WriteString(p[:i])
			p = p[i:]
		}

		if peekByte(p) != ',' {
			break
		}
		p = p[1:]
		hostBuf.WriteByte(',')
		portBuf.WriteByte(',')
	}

	// The joined lists are percent-decoded as a unit, after joining. This
	// means %2C decodes to a comma that later splits like a real separator --
	// a host name containing a literal comma is inexpressible, as in libpq.
	if hostBuf.Len() > 0 {
		hostList, err := uriDecode(hostBuf.String(), "")
		if err != nil {
			return nil, meta, err
		}
		settings["host"] = hostList
	}
	// Like libpq, the port list is stored even when every element is empty
	// (postgres://h1,h2 yields port=","). This matters: a connection string
	// port shadows PGPORT during settings merge, so with PGPORT=1,2 a
	// three-host URI without ports must use the default port for every host,
	// not fail the port/host count check. Such an implied all-empty list
	// carries no user-supplied port text, which is what
	// meta.impliedEmptyPortList reports -- judged on the raw bytes, before
	// percent-decoding trims spaces, so a port the user actually typed never
	// counts as implied even when it decodes to empty.
	if portBuf.Len() > 0 {
		meta.impliedEmptyPortList = strings.Trim(portBuf.String(), ",") == ""
		portList, err := uriDecode(portBuf.String(), "")
		if err != nil {
			return nil, meta, err
		}
		settings["port"] = portList
	}

	if peekByte(p) == '/' {
		p = p[1:]
		dbname := p
		p = ""
		if i := strings.IndexByte(dbname, '?'); i >= 0 {
			p = dbname[i:]
			dbname = dbname[:i]
		}
		// Like libpq, an empty path component leaves dbname unset so the
		// default stays in effect.
		if dbname != "" {
			val, err := uriDecode(dbname, "")
			if err != nil {
				return nil, meta, err
			}
			settings["database"] = val
		}
	}

	if peekByte(p) == '?' {
		// Query parameters land in their own map first: an explicit ?port=
		// overrides the netloc-derived port list and is user-supplied port
		// text, so it cancels the implied-list exemption even when its value
		// is empty.
		query := make(map[string]string)
		if err := parseURLQueryParams(p[1:], query, &meta); err != nil {
			return nil, meta, err
		}
		if _, ok := query["port"]; ok {
			meta.impliedEmptyPortList = false
		}
		maps.Copy(settings, query)
	}

	return settings, meta, nil
}

// peekByte returns the first byte of s, or 0 if s is empty. A NUL byte is a
// safe end-of-input sentinel because parseURLSettings rejects literal NULs.
func peekByte(s string) byte {
	if len(s) == 0 {
		return 0
	}
	return s[0]
}

// parseURLQueryParams parses the query part of a connection URI into settings,
// mirroring libpq's conninfo_uri_parse_params: '&'-separated pairs, exactly
// one raw '=' per pair, both halves percent-decoded, last occurrence wins. It
// records in meta which spellings of the ssl/sslmode keys it saw (see
// parseURLMeta).
func parseURLQueryParams(params string, settings map[string]string, meta *parseURLMeta) error {
	// sslWasLast records whether ssl or sslmode was encountered most recently.
	// This matters if the final repeated ssl value is "true": the alias wins
	// only when it occurs after the final explicit sslmode.
	sslWasLast := false

	for params != "" {
		pair := params
		if i := strings.IndexByte(params, '&'); i >= 0 {
			pair = params[:i]
			params = params[i+1:]
		} else {
			params = ""
		}

		rawKey, rawValue, found := strings.Cut(pair, "=")
		if !found {
			return fmt.Errorf(`missing key/value separator "=" in URI query parameter: "%s"`, rawKey)
		}
		if strings.IndexByte(rawValue, '=') >= 0 {
			return fmt.Errorf(`extra key/value separator "=" in URI query parameter: "%s"`, rawKey)
		}

		key, err := uriDecode(rawKey, "")
		if err != nil {
			return err
		}
		key = canonicalConnStringKey(key)

		secretName := ""
		if key == "password" || key == "sslpassword" {
			secretName = key
		}
		value, err := uriDecode(rawValue, secretName)
		if err != nil {
			return err
		}

		// Resolve repeated ssl values under their raw key before applying the
		// JDBC compatibility alias. Otherwise ssl=true would overwrite an
		// independent explicit sslmode that must reappear if a later ssl value
		// supersedes the alias. The alias is URI-only and applies only to the
		// literal value "true".
		switch key {
		case "ssl":
			sslWasLast = true
			if value == "true" {
				meta.sawSSLTrueAlias = true
			} else {
				meta.sawRawSSLKey = true
			}
		case "sslmode":
			sslWasLast = false
			meta.sawExplicitSSLModeKey = true
		}

		settings[key] = value
	}

	if value, ok := settings["ssl"]; ok && value == "true" {
		delete(settings, "ssl")
		if sslWasLast {
			settings["sslmode"] = "require"
		}
	}

	return nil
}

// uriDecode percent-decodes a URI component with the same rules as libpq's
// conninfo_uri_decode: '%' followed by exactly two case-insensitive hex
// digits, %00 forbidden, leading and trailing ASCII spaces skipped, interior
// spaces rejected. Decoded bytes are emitted raw with no character set
// validation.
//
// If secretName is non-empty, error messages identify that component by name
// instead of quoting its raw value. This prevents the known component's raw
// contents from being copied into the decode error; it does not make the
// separate, best-effort redaction of an invalid connection string complete.
func uriDecode(raw, secretName string) (string, error) {
	// The component may be a secret; every error must be built by fail so the
	// quote-the-raw-value-or-not redaction decision lives in exactly one place.
	fail := func(secretFormat, publicFormat string) (string, error) {
		if secretName != "" {
			return "", fmt.Errorf(secretFormat, secretName)
		}
		return "", fmt.Errorf(publicFormat, raw)
	}

	var b strings.Builder
	b.Grow(len(raw))

	i := 0
	for i < len(raw) && raw[i] == ' ' {
		i++
	}
	for i < len(raw) && raw[i] != ' ' {
		if raw[i] != '%' {
			b.WriteByte(raw[i])
			i++
			continue
		}

		var hi, lo byte
		var ok1, ok2 bool
		if i+1 < len(raw) {
			hi, ok1 = hexDigit(raw[i+1])
		}
		if i+2 < len(raw) {
			lo, ok2 = hexDigit(raw[i+2])
		}
		if !ok1 || !ok2 {
			return fail("invalid percent-encoded token in %s", `invalid percent-encoded token: "%s"`)
		}

		c := hi<<4 | lo
		if c == 0 {
			return fail("forbidden value %%00 in percent-encoded value in %s", `forbidden value %%00 in percent-encoded value: "%s"`)
		}
		b.WriteByte(c)
		i += 3
	}
	for i < len(raw) && raw[i] == ' ' {
		i++
	}
	if i < len(raw) {
		return fail("unexpected spaces found in %s, use percent-encoded spaces (%%20) instead", `unexpected spaces found in "%s", use percent-encoded spaces (%%20) instead`)
	}

	return b.String(), nil
}

// uriDecodeLenient is a non-failing variant of uriDecode used for best-effort
// redaction: valid percent-encodings are decoded, anything uriDecode would
// reject passes through unchanged. Redaction processing itself must not fail,
// and it must see the same key spelling the parser would (pass%77ord decodes
// to password), so encoded password keys are recognized even in connection
// strings that do not parse. This helps recognize keys but cannot make
// redaction complete when malformed input has ambiguous component boundaries.
func uriDecodeLenient(raw string) string {
	raw = strings.Trim(raw, " ")

	var b strings.Builder
	b.Grow(len(raw))

	for i := 0; i < len(raw); {
		if raw[i] == '%' && i+2 < len(raw) {
			hi, ok1 := hexDigit(raw[i+1])
			lo, ok2 := hexDigit(raw[i+2])
			if ok1 && ok2 {
				b.WriteByte(hi<<4 | lo)
				i += 3
				continue
			}
		}
		b.WriteByte(raw[i])
		i++
	}

	return b.String()
}

func hexDigit(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	}
	return 0, false
}
