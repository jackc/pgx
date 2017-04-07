package sanitize

import (
	"unicode/utf8"
)

// Part is either a string or an int. A string is raw SQL. An int is a
// argument placeholder.
type Part interface{}

type Query struct {
	Parts []Part
}

func (q *Query) Sanitize(args ...interface{}) (string, error) {
	return "", nil
}

func NewQuery(sql string) (*Query, error) {
	l := &sqlLexer{
		src:     sql,
		stateFn: rawState,
	}

	for l.stateFn != nil {
		l.stateFn = l.stateFn(l)
	}

	query := &Query{Parts: l.parts}

	return query, nil
}

type sqlLexer struct {
	src     string
	start   int
	pos     int
	stateFn stateFn
	parts   []Part
}

type stateFn func(*sqlLexer) stateFn

func rawState(l *sqlLexer) stateFn {
	for {
		r, width := utf8.DecodeRuneInString(l.src[l.pos:])
		l.pos += width

		switch r {
		case '\'':
			return singleQuoteState
		case '"':
			return doubleQuoteState
		case '$':
			nextRune, _ := utf8.DecodeRuneInString(l.src[l.pos:])
			if '0' <= nextRune && nextRune <= '9' {
				if l.pos-l.start > 0 {
					l.parts = append(l.parts, l.src[l.start:l.pos-width])
				}
				l.start = l.pos
				return placeholderState
			}
		case utf8.RuneError:
			if l.pos-l.start > 0 {
				l.parts = append(l.parts, l.src[l.start:l.pos])
				l.start = l.pos
			}
			return nil
		}
	}
}

func singleQuoteState(l *sqlLexer) stateFn {
	for {
		r, width := utf8.DecodeRuneInString(l.src[l.pos:])
		l.pos += width

		switch r {
		case '\'':
			nextRune, width := utf8.DecodeRuneInString(l.src[l.pos:])
			if nextRune != '\'' {
				return rawState
			}
			l.pos += width
		case utf8.RuneError:
			if l.pos-l.start > 0 {
				l.parts = append(l.parts, l.src[l.start:l.pos])
				l.start = l.pos
			}
			return nil
		}
	}
}

func doubleQuoteState(l *sqlLexer) stateFn {
	for {
		r, width := utf8.DecodeRuneInString(l.src[l.pos:])
		l.pos += width

		switch r {
		case '"':
			nextRune, width := utf8.DecodeRuneInString(l.src[l.pos:])
			if nextRune != '"' {
				return rawState
			}
			l.pos += width
		case utf8.RuneError:
			if l.pos-l.start > 0 {
				l.parts = append(l.parts, l.src[l.start:l.pos])
				l.start = l.pos
			}
			return nil
		}
	}
}

// placeholderState consumes a placeholder value. The $ must have already has
// already been consumed. The first rune must be a digit.
func placeholderState(l *sqlLexer) stateFn {
	num := 0

	for {
		r, width := utf8.DecodeRuneInString(l.src[l.pos:])
		l.pos += width

		if '0' <= r && r <= '9' {
			num *= 10
			num += int(r - '0')
		} else {
			l.parts = append(l.parts, num)
			l.pos -= width
			l.start = l.pos
			return rawState
		}
	}
}

// SanitizeSQL replaces placeholder values with args. It quotes and escapes args
// as necessary. This function is only safe when standard_conforming_strings is
// on.
func SanitizeSQL(sql string, args ...interface{}) (string, error) {
	query, err := NewQuery(sql)
	if err != nil {
		return "", err
	}
	return query.Sanitize(args...)
}
