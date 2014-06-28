package pgx_test

import (
	"fmt"
	"github.com/jackc/pgx"
	"regexp"
	"strconv"
)

const (
	pointOid = 600
)

var pointRegexp *regexp.Regexp = regexp.MustCompile(`^\((.*),(.*)\)$`)

type Point struct {
	x float64
	y float64
}

func (p Point) String() string {
	return fmt.Sprintf("%v, %v", p.x, p.y)
}

func Example_customValueTranscoder() {
	pgx.ValueTranscoders[pointOid] = &pgx.ValueTranscoder{
		Decode: func(qr *pgx.QueryResult, fd *pgx.FieldDescription, size int32) interface{} {
			return decodePoint(qr, fd, size)
		},
		EncodeTo: encodePoint}

	conn, err := pgx.Connect(*defaultConnConfig)
	if err != nil {
		fmt.Printf("Unable to establish connection: %v", err)
		return
	}

	v, _ := conn.SelectValue("select point(1.5,2.5)")
	fmt.Println(v)
	// Output:
	// 1.5, 2.5
}

func decodePoint(qr *pgx.QueryResult, fd *pgx.FieldDescription, size int32) Point {
	var p Point

	if fd.DataType != pointOid {
		qr.Fatal(pgx.ProtocolError(fmt.Sprintf("Tried to read point but received: %v", fd.DataType)))
		return p
	}

	switch fd.FormatCode {
	case pgx.TextFormatCode:
		s := qr.MessageReader().ReadString(size)
		match := pointRegexp.FindStringSubmatch(s)
		if match == nil {
			qr.Fatal(pgx.ProtocolError(fmt.Sprintf("Received invalid point: %v", s)))
			return p
		}

		var err error
		p.x, err = strconv.ParseFloat(match[1], 64)
		if err != nil {
			qr.Fatal(pgx.ProtocolError(fmt.Sprintf("Received invalid point: %v", s)))
			return p
		}
		p.y, err = strconv.ParseFloat(match[2], 64)
		if err != nil {
			qr.Fatal(pgx.ProtocolError(fmt.Sprintf("Received invalid point: %v", s)))
			return p
		}
		return p
	default:
		qr.Fatal(pgx.ProtocolError(fmt.Sprintf("Unknown field description format code: %v", fd.FormatCode)))
		return p
	}
}

func encodePoint(w *pgx.WriteBuf, value interface{}) error {
	p, ok := value.(Point)
	if !ok {
		return fmt.Errorf("Expected Point, received %T", value)
	}

	s := fmt.Sprintf("point(%v,%v)", p.x, p.y)
	w.WriteInt32(int32(len(s)))
	w.WriteBytes([]byte(s))

	return nil
}
