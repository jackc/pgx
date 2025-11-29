package sanitize

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"time"
	"unicode/utf8"
)

// Part is either a string or an int. A string is raw SQL. An int is a
// argument placeholder.
type Part any

type Query struct {
	Parts []Part
}

// utf.DecodeRune returns the utf8.RuneError for errors. But that is actually rune U+FFFD -- the unicode replacement
// character. utf8.RuneError is not an error if it is also width 3.
//
// https://github.com/jackc/pgx/issues/1380
const replacementcharacterwidth = 3

const maxBufSize = 16384 // 16 Ki

var bufPool = &pool[*bytes.Buffer]{
	new: func() *bytes.Buffer {
		return &bytes.Buffer{}
	},
	reset: func(b *bytes.Buffer) bool {
		n := b.Len()
		b.Reset()
		return n < maxBufSize
	},
}

var null = []byte("null")

func (q *Query) Sanitize(args ...any) (string, error) {
	argUse := make([]bool, len(args))
	buf := bufPool.get()
	defer bufPool.put(buf)

	for _, part := range q.Parts {
		switch part := part.(type) {
		case string:
			buf.WriteString(part)
		case int:
			argIdx := part - 1
			var p []byte
			if argIdx < 0 {
				return "", fmt.Errorf("first sql argument must be > 0")
			}

			if argIdx >= len(args) {
				return "", fmt.Errorf("insufficient arguments")
			}

			// Prevent SQL injection via Line Comment Creation
			// https://github.com/jackc/pgx/security/advisories/GHSA-m7wr-2xf7-cm9p
			buf.WriteByte(' ')

			arg := args[argIdx]
			switch arg := arg.(type) {
			case nil:
				p = null
			case int64:
				p = strconv.AppendInt(buf.AvailableBuffer(), arg, 10)
			case float64:
				p = strconv.AppendFloat(buf.AvailableBuffer(), arg, 'f', -1, 64)
			case bool:
				p = strconv.AppendBool(buf.AvailableBuffer(), arg)
			case []byte:
				p = QuoteBytes(buf.AvailableBuffer(), arg)
			case string:
				p = QuoteString(buf.AvailableBuffer(), arg)
			case time.Time:
				p = arg.Truncate(time.Microsecond).
					AppendFormat(buf.AvailableBuffer(), "'2006-01-02 15:04:05.999999999Z07:00:00'")
			default:
				return "", fmt.Errorf("invalid arg type: %T", arg)
			}
			argUse[argIdx] = true

			buf.Write(p)

			// Prevent SQL injection via Line Comment Creation
			// https://github.com/jackc/pgx/security/advisories/GHSA-m7wr-2xf7-cm9p
			buf.WriteByte(' ')
		default:
			return "", fmt.Errorf("invalid Part type: %T", part)
		}
	}

	for i, used := range argUse {
		if !used {
			return "", fmt.Errorf("unused argument: %d", i)
		}
	}
	return buf.String(), nil
}

func NewQuery(sql string) (*Query, error) {
	query := &Query{}
	query.init(sql)

	return query, nil
}

var sqlLexerPool = &pool[*sqlLexer]{
	new: func() *sqlLexer {
		return &sqlLexer{}
	},
	reset: func(sl *sqlLexer) bool {
		*sl = sqlLexer{}
		return true
	},
}

func (q *Query) init(sql string) {
	parts := q.Parts[:0]
	if parts == nil {
		// dirty, but fast heuristic to preallocate for ~90% usecases
		n := 1
		for i := 0; i < len(sql); i++ {
			if sql[i] == '$' {
				n++
			} else if sql[i] == '-' && i+1 < len(sql) && sql[i+1] == '-' {
				n++
			}
		}
		parts = make([]Part, 0, n)
	}

	l := sqlLexerPool.get()
	defer sqlLexerPool.put(l)

	l.src = sql
	l.stateFn = rawState
	l.parts = parts

	for l.stateFn != nil {
		l.stateFn = l.stateFn(l)
	}

	q.Parts = l.parts
}

func QuoteString(dst []byte, str string) []byte {
	const quote = '\''

	if len(str) <= 64 {
		// Short strings: use worst-case allocation (avoids double-scan overhead)
		dst = slices.Grow(dst, len(str)*2+2)
		dst = append(dst, quote)
		for i := 0; i < len(str); i++ {
			if str[i] == quote {
				dst = append(dst, quote, quote)
			} else {
				dst = append(dst, str[i])
			}
		}
		dst = append(dst, quote)
		return dst
	}

	// Long strings: scan first to allocate exact size
	quoteCount := 0
	for i := 0; i < len(str); i++ {
		if str[i] == quote {
			quoteCount++
		}
	}

	// Preallocate space for exact size
	dst = slices.Grow(dst, len(str)+quoteCount+2)

	// Add opening quote
	dst = append(dst, quote)

	// Iterate through the string without allocating
	if quoteCount == 0 {
		dst = append(dst, str...)
	} else {
		for i := 0; i < len(str); i++ {
			if str[i] == quote {
				dst = append(dst, quote, quote)
			} else {
				dst = append(dst, str[i])
			}
		}
	}

	// Add closing quote
	dst = append(dst, quote)

	return dst
}

func QuoteBytes(dst, buf []byte) []byte {
	if len(buf) == 0 {
		return append(dst, `'\x'`...)
	}

	// Calculate required length
	requiredLen := 3 + hex.EncodedLen(len(buf)) + 1

	// Ensure dst has enough capacity
	if cap(dst)-len(dst) < requiredLen {
		newDst := make([]byte, len(dst), len(dst)+requiredLen)
		copy(newDst, dst)
		dst = newDst
	}

	// Record original length and extend slice
	origLen := len(dst)
	dst = dst[:origLen+requiredLen]

	// Add prefix
	dst[origLen] = '\''
	dst[origLen+1] = '\\'
	dst[origLen+2] = 'x'

	// Encode bytes directly into dst
	hex.Encode(dst[origLen+3:len(dst)-1], buf)

	// Add suffix
	dst[len(dst)-1] = '\''

	return dst
}

type sqlLexer struct {
	src     string
	start   int
	pos     int
	nested  int // multiline comment nesting level.
	stateFn stateFn
	parts   []Part
}

type stateFn func(*sqlLexer) stateFn

func rawState(l *sqlLexer) stateFn {
	for {
		// ASCII fast-path
		if l.pos < len(l.src) && l.src[l.pos] < 128 {
			c := l.src[l.pos]
			l.pos++

			switch c {
			case 'e', 'E':
				if l.pos < len(l.src) && l.src[l.pos] == '\'' {
					l.pos++
					return escapeStringState
				}
			case '\'':
				return singleQuoteState
			case '"':
				return doubleQuoteState
			case '$':
				if l.pos < len(l.src) {
					next := l.src[l.pos]
					if next >= '0' && next <= '9' {
						if l.pos-l.start > 0 {
							l.parts = append(l.parts, l.src[l.start:l.pos-1])
						}
						l.start = l.pos
						return placeholderState
					}
				}
			case '-':
				if l.pos < len(l.src) && l.src[l.pos] == '-' {
					l.pos++
					return oneLineCommentState
				}
			case '/':
				if l.pos < len(l.src) && l.src[l.pos] == '*' {
					l.pos++
					return multilineCommentState
				}
			}
			continue
		}

		r, width := utf8.DecodeRuneInString(l.src[l.pos:])
		l.pos += width

		switch r {
		case 'e', 'E':
			if l.pos < len(l.src) && l.src[l.pos] == '\'' {
				l.pos++
				return escapeStringState
			}
		case '\'':
			return singleQuoteState
		case '"':
			return doubleQuoteState
		case '$':
			if l.pos < len(l.src) {
				next := l.src[l.pos]
				if next >= '0' && next <= '9' {
					if l.pos-l.start > 0 {
						l.parts = append(l.parts, l.src[l.start:l.pos-width])
					}
					l.start = l.pos
					return placeholderState
				}
			}
		case '-':
			if l.pos < len(l.src) && l.src[l.pos] == '-' {
				l.pos++
				return oneLineCommentState
			}
		case '/':
			if l.pos < len(l.src) && l.src[l.pos] == '*' {
				l.pos++
				return multilineCommentState
			}
		case utf8.RuneError:
			if width != replacementcharacterwidth {
				if l.pos-l.start > 0 {
					l.parts = append(l.parts, l.src[l.start:l.pos])
					l.start = l.pos
				}
				return nil
			}
		}
	}
}

func singleQuoteState(l *sqlLexer) stateFn {
	for {
		// ASCII fast-path
		if l.pos < len(l.src) && l.src[l.pos] < 128 {
			c := l.src[l.pos]
			l.pos++

			if c == '\'' {
				if l.pos < len(l.src) && l.src[l.pos] == '\'' {
					l.pos++
					continue
				}
				return rawState
			}
			continue
		}

		r, width := utf8.DecodeRuneInString(l.src[l.pos:])
		l.pos += width

		switch r {
		case '\'':
			if l.pos < len(l.src) && l.src[l.pos] == '\'' {
				l.pos++
			} else {
				return rawState
			}
		case utf8.RuneError:
			if width != replacementcharacterwidth {
				if l.pos-l.start > 0 {
					l.parts = append(l.parts, l.src[l.start:l.pos])
					l.start = l.pos
				}
				return nil
			}
		}
	}
}

func doubleQuoteState(l *sqlLexer) stateFn {
	for {
		// ASCII fast-path
		if l.pos < len(l.src) && l.src[l.pos] < 128 {
			c := l.src[l.pos]
			l.pos++

			if c == '"' {
				if l.pos < len(l.src) && l.src[l.pos] == '"' {
					l.pos++
					continue
				}
				return rawState
			}
			continue
		}

		r, width := utf8.DecodeRuneInString(l.src[l.pos:])
		l.pos += width

		switch r {
		case '"':
			if l.pos < len(l.src) && l.src[l.pos] == '"' {
				l.pos++
			} else {
				return rawState
			}
		case utf8.RuneError:
			if width != replacementcharacterwidth {
				if l.pos-l.start > 0 {
					l.parts = append(l.parts, l.src[l.start:l.pos])
					l.start = l.pos
				}
				return nil
			}
		}
	}
}

// placeholderState consumes a placeholder value. The $ must have already has
// already been consumed. The first rune must be a digit.
func placeholderState(l *sqlLexer) stateFn {
	num := 0

	for {
		if l.pos < len(l.src) {
			c := l.src[l.pos]
			if c >= '0' && c <= '9' {
				l.pos++
				num *= 10
				num += int(c - '0')
				continue
			}
		}

		l.parts = append(l.parts, num)
		l.start = l.pos
		return rawState
	}
}

func escapeStringState(l *sqlLexer) stateFn {
	for {
		r, width := utf8.DecodeRuneInString(l.src[l.pos:])
		l.pos += width

		switch r {
		case '\\':
			_, width = utf8.DecodeRuneInString(l.src[l.pos:])
			l.pos += width
		case '\'':
			if l.pos < len(l.src) && l.src[l.pos] == '\'' {
				l.pos++
			} else {
				return rawState
			}
		case utf8.RuneError:
			if width != replacementcharacterwidth {
				if l.pos-l.start > 0 {
					l.parts = append(l.parts, l.src[l.start:l.pos])
					l.start = l.pos
				}
				return nil
			}
		}
	}
}

func oneLineCommentState(l *sqlLexer) stateFn {
	for {
		// ASCII fast-path
		if l.pos < len(l.src) && l.src[l.pos] < 128 {
			c := l.src[l.pos]
			l.pos++

			if c == '\n' || c == '\r' {
				return rawState
			}

			// Backslash needs to consume next char
			if c == '\\' && l.pos < len(l.src) {
				l.pos++
			}
			continue
		}

		// Non-ASCII: use UTF-8 decoding
		r, width := utf8.DecodeRuneInString(l.src[l.pos:])
		l.pos += width

		switch r {
		case '\\':
			_, width = utf8.DecodeRuneInString(l.src[l.pos:])
			l.pos += width
		case '\n', '\r':
			return rawState
		case utf8.RuneError:
			if width != replacementcharacterwidth {
				if l.pos-l.start > 0 {
					l.parts = append(l.parts, l.src[l.start:l.pos])
					l.start = l.pos
				}
				return nil
			}
		}
	}
}

func multilineCommentState(l *sqlLexer) stateFn {
	for {
		r, width := utf8.DecodeRuneInString(l.src[l.pos:])
		l.pos += width

		switch r {
		case '/':
			if l.pos < len(l.src) && l.src[l.pos] == '*' {
				l.pos++
				l.nested++
			}
		case '*':
			if l.pos < len(l.src) && l.src[l.pos] == '/' {
				l.pos++
				if l.nested == 0 {
					return rawState
				}
				l.nested--
			}
		case utf8.RuneError:
			if width != replacementcharacterwidth {
				if l.pos-l.start > 0 {
					l.parts = append(l.parts, l.src[l.start:l.pos])
					l.start = l.pos
				}
				return nil
			}
		}
	}
}

var queryPool = &pool[*Query]{
	new: func() *Query {
		return &Query{}
	},
	reset: func(q *Query) bool {
		n := len(q.Parts)
		q.Parts = q.Parts[:0]
		return n < 64 // drop too large queries
	},
}

// SanitizeSQL replaces placeholder values with args. It quotes and escapes args
// as necessary. This function is only safe when standard_conforming_strings is
// on.
func SanitizeSQL(sql string, args ...any) (string, error) {
	query := queryPool.get()
	query.init(sql)
	defer queryPool.put(query)

	return query.Sanitize(args...)
}

type pool[E any] struct {
	p     sync.Pool
	new   func() E
	reset func(E) bool
}

func (pool *pool[E]) get() E {
	v, ok := pool.p.Get().(E)
	if !ok {
		v = pool.new()
	}

	return v
}

func (p *pool[E]) put(v E) {
	if p.reset(v) {
		p.p.Put(v)
	}
}
