package pgx

import (
	"bytes"
	"errors"
	"fmt"
	"unicode"
	"unicode/utf8"
)

const (
	hsPre = iota
	hsKey
	hsSep
	hsVal
	hsNul
	hsNext
	hsEnd
)

type hstoreParser struct {
	str string
	pos int
}

func newHSP(in string) *hstoreParser {
	return &hstoreParser{
		pos: 0,
		str: in,
	}
}

func (p *hstoreParser) Consume() (r rune, end bool) {
	if p.pos >= len(p.str) {
		end = true
		return
	}
	r, w := utf8.DecodeRuneInString(p.str[p.pos:])
	p.pos += w
	return
}

func (p *hstoreParser) Peek() (r rune, end bool) {
	if p.pos >= len(p.str) {
		end = true
		return
	}
	r, _ = utf8.DecodeRuneInString(p.str[p.pos:])
	return
}

func parseHstoreToMap(s string) (m map[string]string, err error) {
	keys, values, err := ParseHstore(s)
	if err != nil {
		return
	}
	m = make(map[string]string, len(keys))
	for i, key := range keys {
		if !values[i].Valid {
			err = fmt.Errorf("key '%s' has NULL value", key)
			m = nil
			return
		}
		m[key] = values[i].String
	}
	return
}

func parseHstoreToNullHstore(s string) (store map[string]NullString, err error) {
	keys, values, err := ParseHstore(s)
	if err != nil {
		return
	}

	store = make(map[string]NullString, len(keys))

	for i, key := range keys {
		store[key] = values[i]
	}
	return
}

// ParseHstore parses the string representation of an hstore column (the same
// you would get from an ordinary SELECT) into two slices of keys and values. it
// is used internally in the default parsing of hstores, but is exported for use
// in handling custom data structures backed by an hstore column without the
// overhead of creating a map[string]string
func ParseHstore(s string) (k []string, v []NullString, err error) {
	if s == "" {
		return
	}

	buf := bytes.Buffer{}
	keys := []string{}
	values := []NullString{}
	p := newHSP(s)

	r, end := p.Consume()
	state := hsPre

	for !end {
		switch state {
		case hsPre:
			if r == '"' {
				state = hsKey
			} else {
				err = errors.New("String does not begin with \"")
			}
		case hsKey:
			switch r {
			case '"': //End of the key
				if buf.Len() == 0 {
					err = errors.New("Empty Key is invalid")
				} else {
					keys = append(keys, buf.String())
					buf = bytes.Buffer{}
					state = hsSep
				}
			case '\\': //Potential escaped character
				n, end := p.Consume()
				switch {
				case end:
					err = errors.New("Found EOS in key, expecting character or \"")
				case n == '"', n == '\\':
					buf.WriteRune(n)
				default:
					buf.WriteRune(r)
					buf.WriteRune(n)
				}
			default: //Any other character
				buf.WriteRune(r)
			}
		case hsSep:
			if r == '=' {
				r, end = p.Consume()
				switch {
				case end:
					err = errors.New("Found EOS after '=', expecting '>'")
				case r == '>':
					r, end = p.Consume()
					switch {
					case end:
						err = errors.New("Found EOS after '=>', expecting '\"' or 'NULL'")
					case r == '"':
						state = hsVal
					case r == 'N':
						state = hsNul
					default:
						err = fmt.Errorf("Invalid character '%c' after '=>', expecting '\"' or 'NULL'", r)
					}
				default:
					err = fmt.Errorf("Invalid character after '=', expecting '>'")
				}
			} else {
				err = fmt.Errorf("Invalid character '%c' after value, expecting '='", r)
			}
		case hsVal:
			switch r {
			case '"': //End of the value
				values = append(values, NullString{String: buf.String(), Valid: true})
				buf = bytes.Buffer{}
				state = hsNext
			case '\\': //Potential escaped character
				n, end := p.Consume()
				switch {
				case end:
					err = errors.New("Found EOS in key, expecting character or \"")
				case n == '"', n == '\\':
					buf.WriteRune(n)
				default:
					buf.WriteRune(r)
					buf.WriteRune(n)
				}
			default: //Any other character
				buf.WriteRune(r)
			}
		case hsNul:
			nulBuf := make([]rune, 3)
			nulBuf[0] = r
			for i := 1; i < 3; i++ {
				r, end = p.Consume()
				if end {
					err = errors.New("Found EOS in NULL value")
					return
				}
				nulBuf[i] = r
			}
			if nulBuf[0] == 'U' && nulBuf[1] == 'L' && nulBuf[2] == 'L' {
				values = append(values, NullString{String: "", Valid: false})
				state = hsNext
			} else {
				err = fmt.Errorf("Invalid NULL value: 'N%s'", string(nulBuf))
			}
		case hsNext:
			if r == ',' {
				r, end = p.Consume()
				switch {
				case end:
					err = errors.New("Found EOS after ',', expcting space")
				case (unicode.IsSpace(r)):
					r, end = p.Consume()
					state = hsKey
				default:
					err = fmt.Errorf("Invalid character '%c' after ', ', expecting \"", r)
				}
			} else {
				err = fmt.Errorf("Invalid character '%c' after value, expecting ','", r)
			}
		}

		if err != nil {
			return
		}
		r, end = p.Consume()
	}
	if state != hsNext {
		err = errors.New("Improperly formatted hstore")
		return
	}
	k = keys
	v = values
	return
}
