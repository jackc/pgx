package pgtype

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/internal/pgio"
)

type TSVectorScanner interface {
	ScanTSVector(TSVector) error
}

type TSVectorValuer interface {
	TSVectorValue() (TSVector, error)
}

// TSVector represents a PostgreSQL tsvector value.
type TSVector struct {
	Lexemes []TSVectorLexeme
	Valid   bool
}

// TSVectorLexeme represents a lexeme within a tsvector, consisting of a word and its positions.
type TSVectorLexeme struct {
	Word      string
	Positions []TSVectorPosition
}

// ScanTSVector implements the [TSVectorScanner] interface.
func (t *TSVector) ScanTSVector(v TSVector) error {
	*t = v
	return nil
}

// TSVectorValue implements the [TSVectorValuer] interface.
func (t TSVector) TSVectorValue() (TSVector, error) {
	return t, nil
}

func (t TSVector) String() string {
	buf, _ := encodePlanTSVectorCodecText{}.Encode(t, nil)
	return string(buf)
}

// Scan implements the [database/sql.Scanner] interface.
func (t *TSVector) Scan(src any) error {
	if src == nil {
		*t = TSVector{}
		return nil
	}

	switch src := src.(type) {
	case string:
		return scanPlanTextAnyToTSVectorScanner{}.scanString(src, t)
	}

	return fmt.Errorf("cannot scan %T", src)
}

// Value implements the [database/sql/driver.Valuer] interface.
func (t TSVector) Value() (driver.Value, error) {
	if !t.Valid {
		return nil, nil
	}

	buf, err := TSVectorCodec{}.PlanEncode(nil, 0, TextFormatCode, t).Encode(t, nil)
	if err != nil {
		return nil, err
	}

	return string(buf), nil
}

// TSVectorWeight represents the weight label of a lexeme position in a tsvector.
type TSVectorWeight byte

const (
	TSVectorWeightA = TSVectorWeight('A')
	TSVectorWeightB = TSVectorWeight('B')
	TSVectorWeightC = TSVectorWeight('C')
	TSVectorWeightD = TSVectorWeight('D')
)

// tsvectorWeightToBinary converts a TSVectorWeight to the 2-bit binary encoding used by PostgreSQL.
func tsvectorWeightToBinary(w TSVectorWeight) uint16 {
	switch w {
	case TSVectorWeightA:
		return 3
	case TSVectorWeightB:
		return 2
	case TSVectorWeightC:
		return 1
	default:
		return 0 // D or unset
	}
}

// tsvectorWeightFromBinary converts a 2-bit binary weight value to a TSVectorWeight.
func tsvectorWeightFromBinary(b uint16) TSVectorWeight {
	switch b {
	case 3:
		return TSVectorWeightA
	case 2:
		return TSVectorWeightB
	case 1:
		return TSVectorWeightC
	default:
		return TSVectorWeightD
	}
}

// TSVectorPosition represents a lexeme position and its optional weight within a tsvector.
type TSVectorPosition struct {
	Position uint16
	Weight   TSVectorWeight
}

func (p TSVectorPosition) String() string {
	s := strconv.FormatUint(uint64(p.Position), 10)
	if p.Weight != 0 && p.Weight != TSVectorWeightD {
		s += string(p.Weight)
	}
	return s
}

type TSVectorCodec struct{}

func (TSVectorCodec) FormatSupported(format int16) bool {
	return format == TextFormatCode || format == BinaryFormatCode
}

func (TSVectorCodec) PreferredFormat() int16 {
	return BinaryFormatCode
}

func (TSVectorCodec) PlanEncode(m *Map, oid uint32, format int16, value any) EncodePlan {
	if _, ok := value.(TSVectorValuer); !ok {
		return nil
	}

	switch format {
	case BinaryFormatCode:
		return encodePlanTSVectorCodecBinary{}
	case TextFormatCode:
		return encodePlanTSVectorCodecText{}
	}

	return nil
}

type encodePlanTSVectorCodecBinary struct{}

func (encodePlanTSVectorCodecBinary) Encode(value any, buf []byte) ([]byte, error) {
	tsv, err := value.(TSVectorValuer).TSVectorValue()
	if err != nil {
		return nil, err
	}

	if !tsv.Valid {
		return nil, nil
	}

	buf = pgio.AppendInt32(buf, int32(len(tsv.Lexemes)))

	for _, entry := range tsv.Lexemes {
		buf = append(buf, entry.Word...)
		buf = append(buf, 0x00)
		buf = pgio.AppendUint16(buf, uint16(len(entry.Positions)))

		// Each position is a uint16: weight (2 bits) | position (14 bits)
		for _, pos := range entry.Positions {
			packed := tsvectorWeightToBinary(pos.Weight)<<14 | pos.Position&0x3FFF
			buf = pgio.AppendUint16(buf, packed)
		}
	}

	return buf, nil
}

type scanPlanBinaryTSVectorToTSVectorScanner struct{}

func (scanPlanBinaryTSVectorToTSVectorScanner) Scan(src []byte, dst any) error {
	scanner := (dst).(TSVectorScanner)

	if src == nil {
		return scanner.ScanTSVector(TSVector{})
	}

	rp := 0

	const (
		uint16Len = 2
		uint32Len = 4
	)

	if len(src[rp:]) < uint32Len {
		return fmt.Errorf("tsvector incomplete %v", src)
	}
	entryCount := int(int32(binary.BigEndian.Uint32(src[rp:])))
	rp += uint32Len

	var tsv TSVector
	if entryCount > 0 {
		tsv.Lexemes = make([]TSVectorLexeme, entryCount)
	}

	for i := range entryCount {
		nullIndex := bytes.IndexByte(src[rp:], 0x00)
		if nullIndex == -1 {
			return fmt.Errorf("invalid tsvector binary format: missing null terminator")
		}

		lexeme := TSVectorLexeme{Word: string(src[rp : rp+nullIndex])}
		rp += nullIndex + 1 // skip past null terminator

		// Read position count.
		if len(src[rp:]) < uint16Len {
			return fmt.Errorf("invalid tsvector binary format: incomplete position count")
		}

		numPositions := int(binary.BigEndian.Uint16(src[rp:]))
		rp += uint16Len

		// Read each packed position: weight (2 bits) | position (14 bits)
		if len(src[rp:]) < numPositions*uint16Len {
			return fmt.Errorf("invalid tsvector binary format: incomplete positions")
		}

		if numPositions > 0 {
			lexeme.Positions = make([]TSVectorPosition, numPositions)
			for pos := range numPositions {
				packed := binary.BigEndian.Uint16(src[rp:])
				rp += uint16Len
				lexeme.Positions[pos] = TSVectorPosition{
					Position: packed & 0x3FFF,
					Weight:   tsvectorWeightFromBinary(packed >> 14),
				}
			}
		}

		tsv.Lexemes[i] = lexeme
	}
	tsv.Valid = true

	return scanner.ScanTSVector(tsv)
}

var tsvectorLexemeReplacer = strings.NewReplacer(
	`\`, `\\`,
	`'`, `\'`,
)

type encodePlanTSVectorCodecText struct{}

func (encodePlanTSVectorCodecText) Encode(value any, buf []byte) ([]byte, error) {
	tsv, err := value.(TSVectorValuer).TSVectorValue()
	if err != nil {
		return nil, err
	}

	if !tsv.Valid {
		return nil, nil
	}

	if buf == nil {
		buf = []byte{}
	}

	for i, lex := range tsv.Lexemes {
		if i > 0 {
			buf = append(buf, ' ')
		}

		buf = append(buf, '\'')
		buf = append(buf, tsvectorLexemeReplacer.Replace(lex.Word)...)
		buf = append(buf, '\'')

		sep := byte(':')
		for _, p := range lex.Positions {
			buf = append(buf, sep)
			buf = append(buf, p.String()...)
			sep = ','
		}
	}

	return buf, nil
}

func (TSVectorCodec) PlanScan(m *Map, oid uint32, format int16, target any) ScanPlan {
	switch format {
	case BinaryFormatCode:
		switch target.(type) {
		case TSVectorScanner:
			return scanPlanBinaryTSVectorToTSVectorScanner{}
		}
	case TextFormatCode:
		switch target.(type) {
		case TSVectorScanner:
			return scanPlanTextAnyToTSVectorScanner{}
		}
	}

	return nil
}

type scanPlanTextAnyToTSVectorScanner struct{}

func (s scanPlanTextAnyToTSVectorScanner) Scan(src []byte, dst any) error {
	scanner := (dst).(TSVectorScanner)

	if src == nil {
		return scanner.ScanTSVector(TSVector{})
	}

	return s.scanString(string(src), scanner)
}

func (scanPlanTextAnyToTSVectorScanner) scanString(src string, scanner TSVectorScanner) error {
	tsv, err := parseTSVector(src)
	if err != nil {
		return err
	}
	return scanner.ScanTSVector(tsv)
}

func (c TSVectorCodec) DecodeDatabaseSQLValue(m *Map, oid uint32, format int16, src []byte) (driver.Value, error) {
	return codecDecodeToTextFormat(c, m, oid, format, src)
}

func (c TSVectorCodec) DecodeValue(m *Map, oid uint32, format int16, src []byte) (any, error) {
	if src == nil {
		return nil, nil
	}

	var tsv TSVector
	err := codecScan(c, m, oid, format, src, &tsv)
	if err != nil {
		return nil, err
	}
	return tsv, nil
}

type tsvectorParser struct {
	str string
	pos int
}

func (p *tsvectorParser) atEnd() bool {
	return p.pos >= len(p.str)
}

func (p *tsvectorParser) peek() byte {
	return p.str[p.pos]
}

func (p *tsvectorParser) consume() (byte, bool) {
	if p.pos >= len(p.str) {
		return 0, true
	}
	b := p.str[p.pos]
	p.pos++
	return b, false
}

func (p *tsvectorParser) consumeSpaces() {
	for !p.atEnd() && p.peek() == ' ' {
		p.consume()
	}
}

// consumeLexeme consumes a single-quoted lexeme, handling single quotes and backslash escapes.
func (p *tsvectorParser) consumeLexeme() (string, error) {
	ch, end := p.consume()
	if end || ch != '\'' {
		return "", fmt.Errorf("invalid tsvector format: lexeme must start with a single quote")
	}

	var buf strings.Builder
	for {
		ch, end := p.consume()
		if end {
			return "", fmt.Errorf("invalid tsvector format: unterminated quoted lexeme")
		}

		switch ch {
		case '\'':
			// Escaped quote ('') — write a literal single quote
			if !p.atEnd() && p.peek() == '\'' {
				p.consume()
				buf.WriteByte('\'')
			} else {
				// Closing quote — lexeme is complete
				return buf.String(), nil
			}
		case '\\':
			next, end := p.consume()
			if end {
				return "", fmt.Errorf("invalid tsvector format: unexpected end after backslash")
			}
			buf.WriteByte(next)
		default:
			buf.WriteByte(ch)
		}
	}
}

// consumePositions consumes a comma-separated list of position[weight] values.
func (p *tsvectorParser) consumePositions() ([]TSVectorPosition, error) {
	var positions []TSVectorPosition

	for {
		pos, err := p.consumePosition()
		if err != nil {
			return nil, err
		}
		positions = append(positions, pos)

		if p.atEnd() || p.peek() != ',' {
			break
		}

		p.consume() // skip ','
	}

	return positions, nil
}

// consumePosition consumes a single position number with optional weight letter.
func (p *tsvectorParser) consumePosition() (TSVectorPosition, error) {
	start := p.pos

	for !p.atEnd() && p.peek() >= '0' && p.peek() <= '9' {
		p.consume()
	}

	if p.pos == start {
		return TSVectorPosition{}, fmt.Errorf("invalid tsvector format: expected position number")
	}

	num, err := strconv.ParseUint(p.str[start:p.pos], 10, 16)
	if err != nil {
		return TSVectorPosition{}, fmt.Errorf("invalid tsvector format: invalid position number %q", p.str[start:p.pos])
	}

	pos := TSVectorPosition{Position: uint16(num), Weight: TSVectorWeightD}

	// Check for optional weight letter
	if !p.atEnd() {
		switch p.peek() {
		case 'A', 'a':
			pos.Weight = TSVectorWeightA
		case 'B', 'b':
			pos.Weight = TSVectorWeightB
		case 'C', 'c':
			pos.Weight = TSVectorWeightC
		case 'D', 'd':
			pos.Weight = TSVectorWeightD
		default:
			return pos, nil
		}
		p.consume()
	}

	return pos, nil
}

// parseTSVector parses a PostgreSQL tsvector text representation.
func parseTSVector(s string) (TSVector, error) {
	result := TSVector{}
	p := &tsvectorParser{str: strings.TrimSpace(s), pos: 0}

	for !p.atEnd() {
		p.consumeSpaces()
		if p.atEnd() {
			break
		}

		word, err := p.consumeLexeme()
		if err != nil {
			return TSVector{}, err
		}

		entry := TSVectorLexeme{Word: word}

		// Check for optional positions after ':'
		if !p.atEnd() && p.peek() == ':' {
			p.consume() // skip ':'

			positions, err := p.consumePositions()
			if err != nil {
				return TSVector{}, err
			}
			entry.Positions = positions
		}

		result.Lexemes = append(result.Lexemes, entry)
	}

	result.Valid = true

	return result, nil
}
