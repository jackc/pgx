package pgtype

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"unicode"
)

type TSVectorCodec struct{}

func (TSVectorCodec) FormatSupported(format int16) bool {
	return format == TextFormatCode || format == BinaryFormatCode
}

func (TSVectorCodec) PreferredFormat() int16 {
	return BinaryFormatCode
}

func (TSVectorCodec) PlanEncode(m *Map, oid uint32, format int16, value any) EncodePlan {
	switch format {
	case BinaryFormatCode:
		switch value.(type) {
		case TSVector:
			return encodePlanTSVectorCodecBinary{}
		}
	case TextFormatCode:
		switch value.(type) {
		case TSVector:
			return encodePlanTSVectorCodecText{}
		}
	}
	return nil
}

type encodePlanTSVectorCodecBinary struct{}

func (encodePlanTSVectorCodecBinary) Encode(value any, buf []byte) (newBuf []byte, err error) {
	tsv := value.(TSVector)
	if !tsv.Valid {
		return nil, nil
	}
	return encodeTSVectorBinary(tsv, buf)
}

type encodePlanTSVectorCodecText struct{}

func (encodePlanTSVectorCodecText) Encode(value any, buf []byte) (newBuf []byte, err error) {
	tsv := value.(TSVector)
	if !tsv.Valid {
		return nil, nil
	}
	return encodeTSVectorText(tsv, buf)
}

func (TSVectorCodec) PlanScan(m *Map, oid uint32, format int16, target any) ScanPlan {
	switch format {
	case BinaryFormatCode:
		switch target.(type) {
		case *TSVector:
			return scanPlanBinaryTSVectorToTSVector{}
		}
	case TextFormatCode:
		switch target.(type) {
		case *TSVector:
			return scanPlanTextTSVectorToTSVector{}
		}
	}
	return nil
}

type scanPlanBinaryTSVectorToTSVector struct{}

func (scanPlanBinaryTSVectorToTSVector) Scan(src []byte, dst any) error {
	if src == nil {
		return (dst.(*TSVector)).ScanTSVector(TSVector{})
	}
	tsv, err := decodeTSVectorBinary(src)
	if err != nil {
		return err
	}
	return (dst.(*TSVector)).ScanTSVector(tsv)
}

type scanPlanTextTSVectorToTSVector struct{}

func (scanPlanTextTSVectorToTSVector) Scan(src []byte, dst any) error {
	if src == nil {
		return (dst.(*TSVector)).ScanTSVector(TSVector{})
	}
	tsv, err := parseTSVectorText(string(src))
	if err != nil {
		return err
	}
	return (dst.(*TSVector)).ScanTSVector(tsv)
}

func (TSVectorCodec) DecodeDatabaseSQLValue(m *Map, oid uint32, format int16, src []byte) (driver.Value, error) {
	if src == nil {
		return nil, nil
	}
	return string(src), nil
}

// Binary encoding/decoding
func encodeTSVectorBinary(tsv TSVector, buf []byte) ([]byte, error) {
	normalized := normalizeTSVector(tsv)
	
	buf = append(buf, 0, 0, 0, 0)
	binary.BigEndian.PutUint32(buf[len(buf)-4:], uint32(len(normalized.Lexemes)))

	for _, lexeme := range normalized.Lexemes {
		if len(lexeme.Word) > 2046 {
			return nil, fmt.Errorf("lexeme too long: %d bytes (max 2046)", len(lexeme.Word))
		}
		
		buf = append(buf, []byte(lexeme.Word)...)
		buf = append(buf, 0)

		if len(lexeme.Positions) > 255 {
			return nil, fmt.Errorf("too many positions: %d (max 255)", len(lexeme.Positions))
		}
		
		buf = append(buf, 0, 0)
		binary.BigEndian.PutUint16(buf[len(buf)-2:], uint16(len(lexeme.Positions)))

		for _, pos := range lexeme.Positions {
			if pos.Position == 0 || pos.Position > 16383 {
				return nil, fmt.Errorf("invalid position %d (must be 1-16383)", pos.Position)
			}
			packed := packWordEntryPos(pos.Position, pos.Weight)
			buf = append(buf, 0, 0)
			binary.BigEndian.PutUint16(buf[len(buf)-2:], packed)
		}
	}

	return buf, nil
}

func decodeTSVectorBinary(src []byte) (TSVector, error) {
	if len(src) < 4 {
		return TSVector{}, fmt.Errorf("invalid tsvector binary")
	}

	numLexemes := binary.BigEndian.Uint32(src[0:4])
	src = src[4:]

	lexemes := make([]Lexeme, numLexemes)

	for i := uint32(0); i < numLexemes; i++ {
		nullIdx := bytes.IndexByte(src, 0)
		if nullIdx == -1 {
			return TSVector{}, fmt.Errorf("invalid format: missing null terminator")
		}

		word := string(src[:nullIdx])
		src = src[nullIdx+1:]

		if len(src) < 2 {
			return TSVector{}, fmt.Errorf("invalid format: missing position count")
		}

		numPositions := binary.BigEndian.Uint16(src[0:2])
		src = src[2:]

		positions := make([]LexemePosition, numPositions)
		for j := uint16(0); j < numPositions; j++ {
			if len(src) < 2 {
				return TSVector{}, fmt.Errorf("invalid format: missing position data")
			}
			packed := binary.BigEndian.Uint16(src[0:2])
			src = src[2:]
			pos, weight := unpackWordEntryPos(packed)
			positions[j] = LexemePosition{Position: pos, Weight: weight}
		}

		lexemes[i] = Lexeme{Word: word, Positions: positions}
	}

	return TSVector{Lexemes: lexemes, Valid: true}, nil
}

func packWordEntryPos(pos uint16, weight Weight) uint16 {
	return (uint16(weight) << 14) | (pos & 0x3FFF)
}

func unpackWordEntryPos(packed uint16) (pos uint16, weight Weight) {
	weight = Weight((packed >> 14) & 0x03)
	pos = packed & 0x3FFF
	return
}

// Text encoding/decoding
func encodeTSVectorText(tsv TSVector, buf []byte) ([]byte, error) {
	normalized := normalizeTSVector(tsv)

	for i, lexeme := range normalized.Lexemes {
		if i > 0 {
			buf = append(buf, ' ')
		}

		needsQuote := needsQuoting(lexeme.Word)
		if needsQuote {
			buf = append(buf, '\'')
			buf = append(buf, escapeLexeme(lexeme.Word)...)
			buf = append(buf, '\'')
		} else {
			buf = append(buf, lexeme.Word...)
		}

		if len(lexeme.Positions) > 0 {
			buf = append(buf, ':')
			for j, pos := range lexeme.Positions {
				if j > 0 {
					buf = append(buf, ',')
				}
				buf = append(buf, fmt.Sprintf("%d", pos.Position)...)
				if pos.Weight != WeightD {
					buf = append(buf, pos.Weight.String()...)
				}
			}
		}
	}

	return buf, nil
}

func parseTSVectorText(s string) (TSVector, error) {
	if s == "" {
		return TSVector{Valid: true}, nil
	}

	parser := &tsvectorParser{input: s, pos: 0}
	lexemes, err := parser.parse()
	if err != nil {
		return TSVector{}, err
	}

	return TSVector{Lexemes: lexemes, Valid: true}, nil
}

type tsvectorParser struct {
	input string
	pos   int
}

func (p *tsvectorParser) parse() ([]Lexeme, error) {
	var lexemes []Lexeme

	for p.pos < len(p.input) {
		p.skipWhitespace()
		if p.pos >= len(p.input) {
			break
		}

		lexeme, err := p.parseLexeme()
		if err != nil {
			return nil, err
		}

		lexemes = append(lexemes, lexeme)
	}

	return lexemes, nil
}

func (p *tsvectorParser) parseLexeme() (Lexeme, error) {
	var word string
	var err error

	if p.peek() == '\'' {
		word, err = p.parseQuotedWord()
	} else {
		word, err = p.parseUnquotedWord()
	}
	if err != nil {
		return Lexeme{}, err
	}

	var positions []LexemePosition
	if p.peek() == ':' {
		p.advance()
		positions, err = p.parsePositions()
		if err != nil {
			return Lexeme{}, err
		}
	}

	return Lexeme{Word: word, Positions: positions}, nil
}

func (p *tsvectorParser) parseQuotedWord() (string, error) {
	if p.peek() != '\'' {
		return "", fmt.Errorf("expected quote at position %d", p.pos)
	}
	p.advance()

	var result strings.Builder
	escaped := false

	for p.pos < len(p.input) {
		ch := p.current()

		if escaped {
			if ch == '\'' || ch == '\\' {
				result.WriteByte(ch)
				escaped = false
			} else {
				return "", fmt.Errorf("invalid escape at position %d", p.pos)
			}
		} else {
			if ch == '\\' {
				escaped = true
			} else if ch == '\'' {
				if p.pos+1 < len(p.input) && p.input[p.pos+1] == '\'' {
					result.WriteByte('\'')
					p.advance()
				} else {
					p.advance()
					return result.String(), nil
				}
			} else {
				result.WriteByte(ch)
			}
		}
		p.advance()
	}

	return "", fmt.Errorf("unterminated quoted string")
}

func (p *tsvectorParser) parseUnquotedWord() (string, error) {
	var result strings.Builder

	for p.pos < len(p.input) {
		ch := p.current()
		if unicode.IsSpace(rune(ch)) || ch == ':' || ch == ',' {
			break
		}
		result.WriteByte(ch)
		p.advance()
	}

	if result.Len() == 0 {
		return "", fmt.Errorf("empty lexeme at position %d", p.pos)
	}

	return result.String(), nil
}

func (p *tsvectorParser) parsePositions() ([]LexemePosition, error) {
	var positions []LexemePosition

	for {
		if !p.isDigit() {
			return nil, fmt.Errorf("expected digit at position %d", p.pos)
		}

		pos, err := p.parseNumber()
		if err != nil {
			return nil, err
		}

		if pos == 0 || pos > 16383 {
			return nil, fmt.Errorf("position %d out of range", pos)
		}

		weight := WeightD
		if p.pos < len(p.input) {
			ch := p.current()
			if ch == 'A' {
				weight = WeightA
				p.advance()
			} else if ch == 'B' {
				weight = WeightB
				p.advance()
			} else if ch == 'C' {
				weight = WeightC
				p.advance()
			} else if ch == 'D' {
				weight = WeightD
				p.advance()
			}
		}

		positions = append(positions, LexemePosition{Position: uint16(pos), Weight: weight})

		if p.peek() == ',' {
			p.advance()
			continue
		}
		break
	}

	return positions, nil
}

func (p *tsvectorParser) parseNumber() (int, error) {
	start := p.pos
	for p.pos < len(p.input) && p.isDigit() {
		p.advance()
	}
	numStr := p.input[start:p.pos]
	return strconv.Atoi(numStr)
}

func (p *tsvectorParser) skipWhitespace() {
	for p.pos < len(p.input) && unicode.IsSpace(rune(p.input[p.pos])) {
		p.advance()
	}
}

func (p *tsvectorParser) current() byte {
	if p.pos >= len(p.input) {
		return 0
	}
	return p.input[p.pos]
}

func (p *tsvectorParser) peek() byte {
	return p.current()
}

func (p *tsvectorParser) advance() {
	p.pos++
}

func (p *tsvectorParser) isDigit() bool {
	ch := p.current()
	return ch >= '0' && ch <= '9'
}

// Helper functions
func needsQuoting(s string) bool {
	if s == "" {
		return true
	}
	for _, r := range s {
		if r == ' ' || r == '\'' || r == '\\' || r == ':' || r == ',' || r < 32 || r > 126 {
			return true
		}
	}
	return false
}

func escapeLexeme(s string) string {
	var buf strings.Builder
	for _, r := range s {
		if r == '\'' {
			buf.WriteRune('\'')
			buf.WriteRune('\'')
		} else if r == '\\' {
			buf.WriteRune('\\')
			buf.WriteRune('\\')
		} else {
			buf.WriteRune(r)
		}
	}
	return buf.String()
}

func normalizeTSVector(tsv TSVector) TSVector {
	if len(tsv.Lexemes) == 0 {
		return tsv
	}

	normalized := TSVector{
		Lexemes: make([]Lexeme, len(tsv.Lexemes)),
		Valid:   tsv.Valid,
	}
	copy(normalized.Lexemes, tsv.Lexemes)

	sort.Slice(normalized.Lexemes, func(i, j int) bool {
		return normalized.Lexemes[i].Word < normalized.Lexemes[j].Word
	})

	for i := range normalized.Lexemes {
		if len(normalized.Lexemes[i].Positions) > 0 {
			sort.Slice(normalized.Lexemes[i].Positions, func(a, b int) bool {
				return normalized.Lexemes[i].Positions[a].Position < normalized.Lexemes[i].Positions[b].Position
			})
		}
	}

	return normalized
}
