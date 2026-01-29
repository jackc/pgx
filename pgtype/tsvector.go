package pgtype

import (
	"database/sql/driver"
	"fmt"
)

const TSVectorOID = 3614
const TSVectorArrayOID = 3643

// Weight represents tsvector position weight (A, B, C, or D)
type Weight uint8

const (
	WeightD Weight = 0 // Default weight
	WeightC Weight = 1
	WeightB Weight = 2
	WeightA Weight = 3
)

// String returns the weight as a string (A, B, C, or D)
func (w Weight) String() string {
	switch w {
	case WeightA:
		return "A"
	case WeightB:
		return "B"
	case WeightC:
		return "C"
	case WeightD:
		return "D"
	default:
		return "D"
	}
}

// LexemePosition represents a position with optional weight
type LexemePosition struct {
	Position uint16 // 1-16383 (14 bits)
	Weight   Weight // A, B, C, or D
}

// Lexeme represents a single lexeme with its positions
type Lexeme struct {
	Word      string
	Positions []LexemePosition
}

// TSVector represents a PostgreSQL tsvector value
type TSVector struct {
	Lexemes []Lexeme
	Valid   bool
}

// ScanTSVector scans a TSVector from a PostgreSQL value
func (dst *TSVector) ScanTSVector(v TSVector) error {
	*dst = v
	return nil
}

// TSVectorValue returns the TSVector
func (src TSVector) TSVectorValue() (TSVector, error) {
	return src, nil
}

// TSVectorScanner is implemented by types that can scan a TSVector
type TSVectorScanner interface {
	ScanTSVector(v TSVector) error
}

// TSVectorValuer is implemented by types that can produce a TSVector
type TSVectorValuer interface {
	TSVectorValue() (TSVector, error)
}

// Scan implements the database/sql Scanner interface
func (dst *TSVector) Scan(src any) error {
	if src == nil {
		*dst = TSVector{}
		return nil
	}

	switch src := src.(type) {
	case string:
		tsv, err := parseTSVectorText(src)
		if err != nil {
			return err
		}
		*dst = tsv
		return nil
	case []byte:
		tsv, err := parseTSVectorText(string(src))
		if err != nil {
			return err
		}
		*dst = tsv
		return nil
	default:
		return fmt.Errorf("cannot scan %T into TSVector", src)
	}
}

// Value implements the database/sql/driver Valuer interface
func (src TSVector) Value() (driver.Value, error) {
	if !src.Valid {
		return nil, nil
	}

	buf := make([]byte, 0, 256)
	buf, err := encodeTSVectorText(src, buf)
	if err != nil {
		return nil, err
	}

	return string(buf), nil
}
