package sanitize

import (
	"strconv"
	"unicode/utf8"
)

const (
	rawState         = iota
	singleQuoteState = iota
	doubleQuoteState = iota
	placeholderState = iota
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
	var start, pos int
	state := rawState

	var query Query

	for {
		r, width := utf8.DecodeRuneInString(sql[pos:])
		pos += width

		switch state {
		case rawState:
			switch r {
			case '\'':
				state = singleQuoteState
			case '"':
				state = doubleQuoteState
			case '$':
				if pos-start > 0 {
					query.Parts = append(query.Parts, sql[start:pos-1])
				}
				start = pos
				state = placeholderState
			case utf8.RuneError:
				if pos-start > 0 {
					query.Parts = append(query.Parts, sql[start:pos])
				}
				return &query, nil
			}
		case singleQuoteState:
			if r == '\'' || r == utf8.RuneError {
				state = rawState
			}
		case doubleQuoteState:
			if r == '"' || r == utf8.RuneError {
				state = rawState
			}
		case placeholderState:
			if r < '0' || r > '9' {
				pos -= width
				if start < pos {
					num, err := strconv.ParseInt(sql[start:pos], 10, 32)
					if err != nil {
						return nil, err
					}
					query.Parts = append(query.Parts, int(num))
				} else {
					query.Parts = append(query.Parts, "$")
				}

				start = pos
				state = rawState
			}
		}
	}

	return &query, nil
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
