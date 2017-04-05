package pgtype

import (
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"

	"github.com/jackc/pgx/pgio"
)

type Vec2 struct {
	X float64
	Y float64
}

type Point struct {
	P      Vec2
	Status Status
}

func (dst *Point) Set(src interface{}) error {
	return fmt.Errorf("cannot convert %v to Point", src)
}

func (dst *Point) Get() interface{} {
	switch dst.Status {
	case Present:
		return dst
	case Null:
		return nil
	default:
		return dst.Status
	}
}

func (src *Point) AssignTo(dst interface{}) error {
	return fmt.Errorf("cannot assign %v to %T", src, dst)
}

func (dst *Point) DecodeText(ci *ConnInfo, src []byte) error {
	if src == nil {
		*dst = Point{Status: Null}
		return nil
	}

	if len(src) < 5 {
		return fmt.Errorf("invalid length for point: %v", len(src))
	}

	parts := strings.SplitN(string(src[1:len(src)-1]), ",", 2)
	if len(parts) < 2 {
		return fmt.Errorf("invalid format for point")
	}

	x, err := strconv.ParseFloat(parts[0], 64)
	if err != nil {
		return err
	}

	y, err := strconv.ParseFloat(parts[1], 64)
	if err != nil {
		return err
	}

	*dst = Point{P: Vec2{x, y}, Status: Present}
	return nil
}

func (dst *Point) DecodeBinary(ci *ConnInfo, src []byte) error {
	if src == nil {
		*dst = Point{Status: Null}
		return nil
	}

	if len(src) != 16 {
		return fmt.Errorf("invalid length for point: %v", len(src))
	}

	x := binary.BigEndian.Uint64(src)
	y := binary.BigEndian.Uint64(src[8:])

	*dst = Point{
		P:      Vec2{math.Float64frombits(x), math.Float64frombits(y)},
		Status: Present,
	}
	return nil
}

func (src *Point) EncodeText(ci *ConnInfo, w io.Writer) (bool, error) {
	switch src.Status {
	case Null:
		return true, nil
	case Undefined:
		return false, errUndefined
	}

	_, err := io.WriteString(w, fmt.Sprintf(`(%f,%f)`, src.P.X, src.P.Y))
	return false, err
}

func (src *Point) EncodeBinary(ci *ConnInfo, w io.Writer) (bool, error) {
	switch src.Status {
	case Null:
		return true, nil
	case Undefined:
		return false, errUndefined
	}

	_, err := pgio.WriteUint64(w, math.Float64bits(src.P.X))
	if err != nil {
		return false, err
	}

	_, err = pgio.WriteUint64(w, math.Float64bits(src.P.Y))
	return false, err
}

// Scan implements the database/sql Scanner interface.
func (dst *Point) Scan(src interface{}) error {
	if src == nil {
		*dst = Point{Status: Null}
		return nil
	}

	switch src := src.(type) {
	case string:
		return dst.DecodeText(nil, []byte(src))
	case []byte:
		return dst.DecodeText(nil, src)
	}

	return fmt.Errorf("cannot scan %T", src)
}

// Value implements the database/sql/driver Valuer interface.
func (src *Point) Value() (driver.Value, error) {
	return encodeValueText(src)
}
