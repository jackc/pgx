package pgconn

import (
	"context"
	"errors"
	"fmt"
	"net"
	"regexp"
	"strings"
)

// SafeToRetry checks if the err is guaranteed to have occurred before sending any data to the server.
func SafeToRetry(err error) bool {
	var retryableErr interface{ SafeToRetry() bool }
	if errors.As(err, &retryableErr) {
		return retryableErr.SafeToRetry()
	}
	return false
}

// Timeout checks if err was caused by a timeout. To be specific, it is true if err was caused within pgconn by a
// context.DeadlineExceeded or an implementer of net.Error where Timeout() is true.
func Timeout(err error) bool {
	var timeoutErr *errTimeout
	return errors.As(err, &timeoutErr)
}

// PgError represents an error reported by the PostgreSQL server. See
// http://www.postgresql.org/docs/current/static/protocol-error-fields.html for
// detailed field description.
type PgError struct {
	Severity            string
	SeverityUnlocalized string
	Code                string
	Message             string
	Detail              string
	Hint                string
	Position            int32
	InternalPosition    int32
	InternalQuery       string
	Where               string
	SchemaName          string
	TableName           string
	ColumnName          string
	DataTypeName        string
	ConstraintName      string
	File                string
	Line                int32
	Routine             string
}

func (pe *PgError) Error() string {
	return pe.Severity + ": " + pe.Message + " (SQLSTATE " + pe.Code + ")"
}

// SQLState returns the SQLState of the error.
func (pe *PgError) SQLState() string {
	return pe.Code
}

// ConnectError is the error returned when a connection attempt fails.
type ConnectError struct {
	Config *Config // The configuration that was used in the connection attempt.
	err    error
}

func (e *ConnectError) Error() string {
	prefix := fmt.Sprintf("failed to connect to `user=%s database=%s`:", e.Config.User, e.Config.Database)
	details := e.err.Error()
	if strings.Contains(details, "\n") {
		return prefix + "\n\t" + strings.ReplaceAll(details, "\n", "\n\t")
	} else {
		return prefix + " " + details
	}
}

func (e *ConnectError) Unwrap() error {
	return e.err
}

type perDialConnectError struct {
	address          string
	originalHostname string
	err              error
}

func (e *perDialConnectError) Error() string {
	return fmt.Sprintf("%s (%s): %s", e.address, e.originalHostname, e.err.Error())
}

func (e *perDialConnectError) Unwrap() error {
	return e.err
}

// ErrConnClosed is returned (possibly wrapped) when an operation is attempted
// on a connection that the driver has already closed, e.g. because a prior
// query was cancelled mid-flight or the underlying socket went away. Use
// errors.Is to test for it, since it shows up wrapped inside connLockError.
var ErrConnClosed = errors.New("conn closed")

type connLockError struct {
	status string
}

func (e *connLockError) SafeToRetry() bool {
	return true // a lock failure by definition happens before the connection is used.
}

func (e *connLockError) Error() string {
	return e.status
}

func (e *connLockError) Unwrap() error {
	if e.status == "conn closed" {
		return ErrConnClosed
	}
	return nil
}

// ParseConfigError is the error returned when a connection string cannot be
// parsed. Its error text masks recognizable passwords on a best-effort basis,
// but malformed input can be too ambiguous to redact completely.
type ParseConfigError struct {
	ConnString string // The original, unredacted connection string that could not be parsed.
	msg        string
	err        error
}

func NewParseConfigError(conn, msg string, err error) error {
	return &ParseConfigError{
		ConnString: conn,
		msg:        msg,
		err:        err,
	}
}

func (e *ParseConfigError) Error() string {
	// redactPW is necessarily best effort: an invalid connection string can be
	// too ambiguous to identify every password. Returning only a static string
	// would be the way to guarantee that Error cannot leak one. The public
	// ConnString field would still allow access to the original string if
	// desired, and Unwrap would allow access to the underlying error.
	connString := redactPW(e.ConnString)
	if e.err == nil {
		return fmt.Sprintf("cannot parse `%s`: %s", connString, e.msg)
	}
	return fmt.Sprintf("cannot parse `%s`: %s (%s)", connString, e.msg, e.err.Error())
}

func (e *ParseConfigError) Unwrap() error {
	return e.err
}

func normalizeTimeoutError(ctx context.Context, err error) error {
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		switch ctx.Err() {
		case context.Canceled:
			// Since the timeout was caused by a context cancellation, the actual error is context.Canceled not the timeout error.
			return context.Canceled
		case context.DeadlineExceeded:
			return &errTimeout{err: ctx.Err()}
		default:
			return &errTimeout{err: err}
		}
	}
	return err
}

type pgconnError struct {
	msg         string
	err         error
	safeToRetry bool
}

func (e *pgconnError) Error() string {
	if e.msg == "" {
		return e.err.Error()
	}
	if e.err == nil {
		return e.msg
	}
	return fmt.Sprintf("%s: %s", e.msg, e.err.Error())
}

func (e *pgconnError) SafeToRetry() bool {
	return e.safeToRetry
}

func (e *pgconnError) Unwrap() error {
	return e.err
}

// errTimeout occurs when an error was caused by a timeout. Specifically, it wraps an error which is
// context.Canceled, context.DeadlineExceeded, or an implementer of net.Error where Timeout() is true.
type errTimeout struct {
	err error
}

func (e *errTimeout) Error() string {
	return fmt.Sprintf("timeout: %s", e.err.Error())
}

func (e *errTimeout) SafeToRetry() bool {
	return SafeToRetry(e.err)
}

func (e *errTimeout) Unwrap() error {
	return e.err
}

type contextAlreadyDoneError struct {
	err error
}

func (e *contextAlreadyDoneError) Error() string {
	return fmt.Sprintf("context already done: %s", e.err.Error())
}

func (e *contextAlreadyDoneError) SafeToRetry() bool {
	return true
}

func (e *contextAlreadyDoneError) Unwrap() error {
	return e.err
}

// newContextAlreadyDoneError double-wraps a context error in `contextAlreadyDoneError` and `errTimeout`.
func newContextAlreadyDoneError(ctx context.Context) (err error) {
	return &errTimeout{&contextAlreadyDoneError{err: ctx.Err()}}
}

// redactPW masks recognizable password fields on a best-effort basis. It
// cannot guarantee redaction when malformed input makes component boundaries
// ambiguous.
func redactPW(connString string) string {
	if strings.HasPrefix(connString, "postgres://") || strings.HasPrefix(connString, "postgresql://") {
		return redactURLPassword(connString)
	}
	quotedKV := regexp.MustCompile(`password='[^']*'`)
	connString = quotedKV.ReplaceAllLiteralString(connString, "password=xxxxx")
	plainKV := regexp.MustCompile(`password=[^ ]*`)
	connString = plainKV.ReplaceAllLiteralString(connString, "password=xxxxx")
	brokenURL := regexp.MustCompile(`:[^:@]+?@`)
	connString = brokenURL.ReplaceAllLiteralString(connString, ":xxxxxx@")
	return connString
}

// redactURLPassword masks recognizable password values in a connection URI.
// For a URI that parses cleanly the component boundaries are certain, and
// every password position -- the userinfo password and password/sslpassword
// query values -- is reliably masked with the rest of the string left intact.
// It is also deliberately usable without a successful parse -- the strings
// that reach ParseConfigError are often exactly the ones that failed to parse
// -- but malformed syntax can make component boundaries ambiguous, so for
// such input redaction is best-effort and may over-mask.
//
// For URI shapes it recognizes, it mirrors parseURLSettings structurally
// rather than pattern-matching the raw text: the userinfo password is whatever
// follows the first ':' before the terminating '@' (found with the parser's
// lookahead), and a query value is masked when its percent-decoded key
// canonicalizes to password or sslpassword. This uses the same decoding as the
// parser, so encoded spellings like pass%77ord are caught and the whole
// recognized raw value is masked no matter what bytes it contains.
func redactURLPassword(connString string) string {
	const mask = "xxxxx"
	var b strings.Builder
	b.Grow(len(connString))

	p := connString
	for _, prefix := range []string{"postgresql://", "postgres://"} {
		if rest, ok := strings.CutPrefix(connString, prefix); ok {
			b.WriteString(prefix)
			p = rest
			break
		}
	}

	// Userinfo: same lookahead as parseURLSettings.
	if i := strings.IndexAny(p, "@/"); i >= 0 && p[i] == '@' {
		user, _, hasPassword := strings.Cut(p[:i], ":")
		p = p[i+1:]
		b.WriteString(user)
		if hasPassword {
			b.WriteString(":" + mask)
		}
		b.WriteByte('@')
	}

	// Nothing after the userinfo is a password position, but a password can
	// land there in malformed URIs: an unencoded '/' or '?' in a userinfo
	// password turns everything after it into path or query, stranding the
	// '@' (postgres://user:pass/word?x=y@host). As a best-effort heuristic,
	// while an '@' remains, mask everything shaped like
	// ":candidate-credential@" across the whole
	// remainder -- before the query is split off, or a stranded '@' inside
	// the query would hide the pattern, and greedily up to each '@', so a
	// ':' inside the stranded password (user:sec:ret@host) cannot split the
	// mask and leak the part before it.
	//
	// The heuristic must not run when the connection string is structurally
	// unambiguous: then nothing can be stranded, a remaining '@' is ordinary
	// data (typically in a query value), and the greedy mask would swallow
	// the '/' and '?' delimiters -- hiding the query from the password-key
	// masking below, so a password=... query parameter would leak. Valid
	// URIs must always redact exactly; the heuristic is reserved for
	// malformed input, where only best effort is possible.
	if strings.IndexByte(p, '@') >= 0 && !uriStructureUnambiguous(connString) {
		brokenUserinfo := regexp.MustCompile(`:[^@]+@`)
		p = brokenUserinfo.ReplaceAllLiteralString(p, ":xxxxxx@")
	}

	qi := uriQueryStart(p)
	if qi < 0 {
		b.WriteString(p)
		return b.String()
	}
	b.WriteString(p[:qi])
	query := p[qi+1:]
	b.WriteByte('?')

	for i, pair := range strings.Split(query, "&") {
		if i > 0 {
			b.WriteByte('&')
		}
		rawKey, _, hasValue := strings.Cut(pair, "=")
		if !hasValue {
			b.WriteString(pair)
			continue
		}
		switch canonicalConnStringKey(uriDecodeLenient(rawKey)) {
		case "password", "sslpassword":
			b.WriteString(rawKey)
			b.WriteByte('=')
			b.WriteString(mask)
		default:
			b.WriteString(pair)
		}
	}
	return b.String()
}

// uriStructureUnambiguous reports whether connString parses as a connection
// URI whose component boundaries are certain, meaning redactURLPassword's
// structural walk is exact and no password bytes can sit outside the two
// positions it masks (the userinfo password and password-keyed query values).
// That requires parseURLSettings to accept the string and every port element
// to be numeric or empty: the parser accepts arbitrary text in the port slot
// (postgres://a@u:sec:ret@h parses with port "sec:ret@h"), so a non-numeric
// port may be a mislaid password and keeps the string in best-effort
// territory.
func uriStructureUnambiguous(connString string) bool {
	settings, _, err := parseURLSettings(connString)
	if err != nil {
		return false
	}
	for part := range strings.SplitSeq(settings["port"], ",") {
		for i := 0; i < len(part); i++ {
			if part[i] < '0' || part[i] > '9' {
				return false
			}
		}
	}
	return true
}

// uriQueryStart returns the index of the '?' that begins the query component
// of p (the post-scheme, post-userinfo part of a connection URI), or -1 if
// there is none. It follows parseURLSettings' structure: '[' at the start of
// a netloc element opens an IPv6 bracket whose contents -- deliberately
// unvalidated -- may contain a literal '?' that is host data, not the query
// delimiter. On an unterminated bracket (the parser errors there) it falls
// back to the first '?' anywhere. With no valid structure to follow,
// over-including text gives the best-effort redactor more candidate pairs to
// inspect.
func uriQueryStart(p string) int {
	i := 0
	for {
		if i < len(p) && p[i] == '[' {
			end := strings.IndexByte(p[i:], ']')
			if end < 0 {
				return strings.IndexByte(p, '?')
			}
			i += end + 1
		}
		// Host and port data: everything up to a '/', '?', or ','.
		for i < len(p) && p[i] != '/' && p[i] != '?' && p[i] != ',' {
			i++
		}
		if i < len(p) && p[i] == ',' {
			i++
			continue
		}
		break
	}
	if i == len(p) {
		return -1
	}
	if p[i] == '?' {
		return i
	}
	// p[i] == '/': a path follows; the first '?' after it starts the query.
	if j := strings.IndexByte(p[i:], '?'); j >= 0 {
		return i + j
	}
	return -1
}

type NotPreferredError struct {
	err         error
	safeToRetry bool
}

func (e *NotPreferredError) Error() string {
	return fmt.Sprintf("standby server not found: %s", e.err.Error())
}

func (e *NotPreferredError) SafeToRetry() bool {
	return e.safeToRetry
}

func (e *NotPreferredError) Unwrap() error {
	return e.err
}

type PrepareError struct {
	err error

	ParseComplete bool // Indicates whether the error occurred after a ParseComplete message was received.
}

func (e *PrepareError) Error() string {
	if e.ParseComplete {
		return fmt.Sprintf("prepare failed after ParseComplete: %s", e.err.Error())
	}
	return e.err.Error()
}

func (e *PrepareError) Unwrap() error {
	return e.err
}
