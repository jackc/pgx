package pgtype

import (
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

	"github.com/jackc/pgio"
	errors "golang.org/x/xerrors"
)

type Vec2 struct {
	X float64
	Y float64
}

type Point struct {
	P      Vec2
	Status Status
}

var nullRE = regexp.MustCompile("^null$")

func (dst *Point) Set(src interface{}) error {
	if src == nil {
		dst.Status = Null
		return nil
	}
	err := errors.Errorf("cannot convert %v to Point", src)
	var p *Point
	switch value := src.(type) {
	case string:
		p, err = parsePoint([]byte(value))
	case []byte:
		if nullRE.Match(value) {
			dst.Status = Null
			return nil
		}
		p, err = parsePoint(value)
	default:
		return err
	}
	if err != nil {
		return err
	}
	*dst = *p
	return nil
}

var pointRE = regexp.MustCompile("^\\(\\d+\\.\\d+,\\s?\\d+\\.\\d+\\)$")
var chunkRE = regexp.MustCompile("\\d+\\.\\d+")

func parsePoint(p []byte) (*Point, error) {
	err := errors.Errorf("cannot parse %s", p)
	if pointRE.Match(p) {
		chunks := chunkRE.FindAll(p, 2)
		if len(chunks) != 2 {
			return nil, err
		}
		x, xErr := strconv.ParseFloat(string(chunks[0]), 64)
		y, yErr := strconv.ParseFloat(string(chunks[1]), 64)
		if xErr != nil || yErr != nil {
			return nil, err
		}
		return &Point{
			P: Vec2{
				X: x,
				Y: y,
			},
			Status: Present,
		}, nil
	} else if nullRE.Match(p) {
		return &Point{
			Status: Null,
		}, nil
	}
	return nil, err
}

func (dst Point) Get() interface{} {
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
	return errors.Errorf("cannot assign %v to %T", src, dst)
}

func (dst *Point) DecodeText(ci *ConnInfo, src []byte) error {
	if src == nil {
		*dst = Point{Status: Null}
		return nil
	}

	if len(src) < 5 {
		return errors.Errorf("invalid length for point: %v", len(src))
	}

	parts := strings.SplitN(string(src[1:len(src)-1]), ",", 2)
	if len(parts) < 2 {
		return errors.Errorf("invalid format for point")
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
		return errors.Errorf("invalid length for point: %v", len(src))
	}

	x := binary.BigEndian.Uint64(src)
	y := binary.BigEndian.Uint64(src[8:])

	*dst = Point{
		P:      Vec2{math.Float64frombits(x), math.Float64frombits(y)},
		Status: Present,
	}
	return nil
}

func (src Point) EncodeText(ci *ConnInfo, buf []byte) ([]byte, error) {
	switch src.Status {
	case Null:
		return nil, nil
	case Undefined:
		return nil, errUndefined
	}

	return append(buf, fmt.Sprintf(`(%s,%s)`,
		strconv.FormatFloat(src.P.X, 'f', -1, 64),
		strconv.FormatFloat(src.P.Y, 'f', -1, 64),
	)...), nil
}

func (src Point) EncodeBinary(ci *ConnInfo, buf []byte) ([]byte, error) {
	switch src.Status {
	case Null:
		return nil, nil
	case Undefined:
		return nil, errUndefined
	}

	buf = pgio.AppendUint64(buf, math.Float64bits(src.P.X))
	buf = pgio.AppendUint64(buf, math.Float64bits(src.P.Y))
	return buf, nil
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
		srcCopy := make([]byte, len(src))
		copy(srcCopy, src)
		return dst.DecodeText(nil, srcCopy)
	}

	return errors.Errorf("cannot scan %T", src)
}

// Value implements the database/sql/driver Valuer interface.
func (src Point) Value() (driver.Value, error) {
	return EncodeValueText(src)
}

func (src Point) MarshalJSON() ([]byte, error) {
	switch src.Status {
	case Present:
		return []byte(fmt.Sprintf("(%g, %g)", src.P.X, src.P.Y)), nil
	case Null:
		return []byte("null"), nil
	case Undefined:
		return nil, errUndefined
	}
	return nil, errBadStatus
}

func (dst *Point) UnmarshalJSON(point []byte) error {
	p, err := parsePoint(point)
	if err != nil {
		return err
	}
	*dst = *p
	return nil
}
