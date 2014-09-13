package pgx_test

import (
	"errors"
	"fmt"
	"github.com/jackc/pgx"
	"regexp"
	"strconv"
)

var pointRegexp *regexp.Regexp = regexp.MustCompile(`^\((.*),(.*)\)$`)

// NullPoint represents a point that may be null.
//
// If Valid is false then the value is NULL.
type NullPoint struct {
	X, Y  float64 // Coordinates of point
	Valid bool    // Valid is true if not NULL
}

func (p *NullPoint) Scan(vr *pgx.ValueReader) error {
	if vr.Type().DataTypeName != "point" {
		return pgx.SerializationError(fmt.Sprintf("NullPoint.Scan cannot decode %s (OID %d)", vr.Type().DataTypeName, vr.Type().DataType))
	}

	if vr.Len() == -1 {
		p.X, p.Y, p.Valid = 0, 0, false
		return nil
	}

	switch vr.Type().FormatCode {
	case pgx.TextFormatCode:
		s := vr.ReadString(vr.Len())
		match := pointRegexp.FindStringSubmatch(s)
		if match == nil {
			return pgx.SerializationError(fmt.Sprintf("Received invalid point: %v", s))
		}

		var err error
		p.X, err = strconv.ParseFloat(match[1], 64)
		if err != nil {
			return pgx.SerializationError(fmt.Sprintf("Received invalid point: %v", s))
		}
		p.Y, err = strconv.ParseFloat(match[2], 64)
		if err != nil {
			return pgx.SerializationError(fmt.Sprintf("Received invalid point: %v", s))
		}
	case pgx.BinaryFormatCode:
		return errors.New("binary format not implemented")
	default:
		return fmt.Errorf("unknown format %v", vr.Type().FormatCode)
	}

	p.Valid = true
	return vr.Err()
}

func (p NullPoint) FormatCode() int16 { return pgx.BinaryFormatCode }

func (p NullPoint) Encode(w *pgx.WriteBuf, oid pgx.Oid) error {
	if !p.Valid {
		w.WriteInt32(-1)
		return nil
	}

	s := fmt.Sprintf("point(%v,%v)", p.X, p.Y)
	w.WriteInt32(int32(len(s)))
	w.WriteBytes([]byte(s))

	return nil
}

func (p NullPoint) String() string {
	if p.Valid {
		return fmt.Sprintf("%v, %v", p.X, p.Y)
	}
	return "null point"
}

func Example_CustomType() {
	conn, err := pgx.Connect(*defaultConnConfig)
	if err != nil {
		fmt.Printf("Unable to establish connection: %v", err)
		return
	}

	var p NullPoint
	err = conn.QueryRow("select null::point").Scan(&p)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(p)

	err = conn.QueryRow("select point(1.5,2.5)").Scan(&p)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(p)
	// Output:
	// null point
	// 1.5, 2.5
}
