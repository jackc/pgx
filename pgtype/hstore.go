package pgtype

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/internal/pgio"
)

type HstoreScanner interface {
	ScanHstore(v Hstore) error
}

type HstoreValuer interface {
	HstoreValue() (Hstore, error)
}

// Hstore represents an hstore column that can be null or have null values
// associated with its keys.
type Hstore map[string]*string

func (h *Hstore) ScanHstore(v Hstore) error {
	*h = v
	return nil
}

func (h Hstore) HstoreValue() (Hstore, error) {
	return h, nil
}

// Scan implements the database/sql Scanner interface.
func (h *Hstore) Scan(src any) error {
	if src == nil {
		*h = nil
		return nil
	}

	switch src := src.(type) {
	case string:
		return scanPlanTextAnyToHstoreScanner{}.Scan([]byte(src), h)
	}

	return fmt.Errorf("cannot scan %T", src)
}

// Value implements the database/sql/driver Valuer interface.
func (h Hstore) Value() (driver.Value, error) {
	if h == nil {
		return nil, nil
	}

	buf, err := HstoreCodec{}.PlanEncode(nil, 0, TextFormatCode, h).Encode(h, nil)
	if err != nil {
		return nil, err
	}
	return string(buf), err
}

type HstoreCodec struct{}

func (HstoreCodec) FormatSupported(format int16) bool {
	return format == TextFormatCode || format == BinaryFormatCode
}

func (HstoreCodec) PreferredFormat() int16 {
	return BinaryFormatCode
}

func (HstoreCodec) PlanEncode(m *Map, oid uint32, format int16, value any) EncodePlan {
	if _, ok := value.(HstoreValuer); !ok {
		return nil
	}

	switch format {
	case BinaryFormatCode:
		return encodePlanHstoreCodecBinary{}
	case TextFormatCode:
		return encodePlanHstoreCodecText{}
	}

	return nil
}

type encodePlanHstoreCodecBinary struct{}

func (encodePlanHstoreCodecBinary) Encode(value any, buf []byte) (newBuf []byte, err error) {
	hstore, err := value.(HstoreValuer).HstoreValue()
	if err != nil {
		return nil, err
	}

	if hstore == nil {
		return nil, nil
	}

	buf = pgio.AppendInt32(buf, int32(len(hstore)))

	for k, v := range hstore {
		buf = pgio.AppendInt32(buf, int32(len(k)))
		buf = append(buf, k...)

		if v == nil {
			buf = pgio.AppendInt32(buf, -1)
		} else {
			buf = pgio.AppendInt32(buf, int32(len(*v)))
			buf = append(buf, (*v)...)
		}
	}

	return buf, nil
}

type encodePlanHstoreCodecText struct{}

func (encodePlanHstoreCodecText) Encode(value any, buf []byte) (newBuf []byte, err error) {
	hstore, err := value.(HstoreValuer).HstoreValue()
	if err != nil {
		return nil, err
	}

	if hstore == nil {
		return nil, nil
	}

	firstPair := true

	for k, v := range hstore {
		if firstPair {
			firstPair = false
		} else {
			buf = append(buf, ',')
		}

		buf = append(buf, quoteHstoreElementIfNeeded(k)...)
		buf = append(buf, "=>"...)

		if v == nil {
			buf = append(buf, "NULL"...)
		} else {
			buf = append(buf, quoteHstoreElementIfNeeded(*v)...)
		}
	}

	return buf, nil
}

func (HstoreCodec) PlanScan(m *Map, oid uint32, format int16, target any) ScanPlan {

	switch format {
	case BinaryFormatCode:
		switch target.(type) {
		case HstoreScanner:
			return scanPlanBinaryHstoreToHstoreScanner{}
		}
	case TextFormatCode:
		switch target.(type) {
		case HstoreScanner:
			return scanPlanTextAnyToHstoreScanner{}
		}
	}

	return nil
}

type scanPlanBinaryHstoreToHstoreScanner struct{}

func (scanPlanBinaryHstoreToHstoreScanner) Scan(src []byte, dst any) error {
	scanner := (dst).(HstoreScanner)

	if src == nil {
		return scanner.ScanHstore(Hstore{})
	}

	rp := 0

	if len(src[rp:]) < 4 {
		return fmt.Errorf("hstore incomplete %v", src)
	}
	pairCount := int(int32(binary.BigEndian.Uint32(src[rp:])))
	rp += 4

	hstore := make(Hstore, pairCount)

	for i := 0; i < pairCount; i++ {
		if len(src[rp:]) < 4 {
			return fmt.Errorf("hstore incomplete %v", src)
		}
		keyLen := int(int32(binary.BigEndian.Uint32(src[rp:])))
		rp += 4

		if len(src[rp:]) < keyLen {
			return fmt.Errorf("hstore incomplete %v", src)
		}
		key := string(src[rp : rp+keyLen])
		rp += keyLen

		if len(src[rp:]) < 4 {
			return fmt.Errorf("hstore incomplete %v", src)
		}
		valueLen := int(int32(binary.BigEndian.Uint32(src[rp:])))
		rp += 4

		var valueBuf []byte
		if valueLen >= 0 {
			valueBuf = src[rp : rp+valueLen]
			rp += valueLen
		}

		var value Text
		err := scanPlanTextAnyToTextScanner{}.Scan(valueBuf, &value)
		if err != nil {
			return err
		}

		if value.Valid {
			hstore[key] = &value.String
		} else {
			hstore[key] = nil
		}
	}

	return scanner.ScanHstore(hstore)
}

type scanPlanTextAnyToHstoreScanner struct{}

func (scanPlanTextAnyToHstoreScanner) Scan(src []byte, dst any) error {
	scanner := (dst).(HstoreScanner)

	if src == nil {
		return scanner.ScanHstore(Hstore{})
	}

	keys, values, err := parseHstore(string(src))
	if err != nil {
		return err
	}

	m := make(Hstore, len(keys))
	for i := range keys {
		if values[i].Valid {
			m[keys[i]] = &values[i].String
		} else {
			m[keys[i]] = nil
		}
	}

	return scanner.ScanHstore(m)
}

func (c HstoreCodec) DecodeDatabaseSQLValue(m *Map, oid uint32, format int16, src []byte) (driver.Value, error) {
	return codecDecodeToTextFormat(c, m, oid, format, src)
}

func (c HstoreCodec) DecodeValue(m *Map, oid uint32, format int16, src []byte) (any, error) {
	if src == nil {
		return nil, nil
	}

	var hstore Hstore
	err := codecScan(c, m, oid, format, src, &hstore)
	if err != nil {
		return nil, err
	}
	return hstore, nil
}

var quoteHstoreReplacer = strings.NewReplacer(`\`, `\\`, `"`, `\"`)

func quoteHstoreElement(src string) string {
	return `"` + quoteArrayReplacer.Replace(src) + `"`
}

func quoteHstoreElementIfNeeded(src string) string {
	if src == "" || (len(src) == 4 && strings.ToLower(src) == "null") || strings.ContainsAny(src, ` {},"\=>`) {
		return quoteArrayElement(src)
	}
	return src
}

const (
	hsPre = iota
	hsKey
	hsSep
	hsVal
	hsNul
	hsNext
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

// parseHstore parses the string representation of an hstore column (the same
// you would get from an ordinary SELECT) into two slices of keys and values. it
// is used internally in the default parsing of hstores.
func parseHstore(s string) (k []string, v []Text, err error) {
	if s == "" {
		return
	}

	buf := bytes.Buffer{}
	keys := []string{}
	values := []Text{}
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
				keys = append(keys, buf.String())
				buf = bytes.Buffer{}
				state = hsSep
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
				values = append(values, Text{String: buf.String(), Valid: true})
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
				values = append(values, Text{})
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
