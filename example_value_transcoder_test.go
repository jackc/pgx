package pgx_test

import (
	"fmt"
	"github.com/JackC/pgx"
	"regexp"
	"strconv"
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
	pgx.ValueTranscoders[pgx.Oid(600)] = &pgx.ValueTranscoder{
		DecodeText: decodePointFromText,
		EncodeTo:   encodePoint}

	conn, err := pgx.Connect(*defaultConnectionParameters)
	if err != nil {
		fmt.Printf("Unable to establish connection: %v", err)
		return
	}

	v, _ := conn.SelectValue("select point(1.5,2.5)")
	fmt.Println(v)
	// Output:
	// 1.5, 2.5
}

func decodePointFromText(mr *pgx.MessageReader, size int32) interface{} {
	s := mr.ReadString(size)
	match := pointRegexp.FindStringSubmatch(s)
	if match == nil {
		return pgx.ProtocolError(fmt.Sprintf("Received invalid point: %v", s))
	}

	var err error
	var p Point
	p.x, err = strconv.ParseFloat(match[1], 64)
	if err != nil {
		return pgx.ProtocolError(fmt.Sprintf("Received invalid point: %v", s))
	}
	p.y, err = strconv.ParseFloat(match[2], 64)
	if err != nil {
		return pgx.ProtocolError(fmt.Sprintf("Received invalid point: %v", s))
	}
	return p
}

func encodePoint(w *pgx.MessageWriter, value interface{}) {
	p := value.(Point)
	s := fmt.Sprintf("point(%v,%v)", p.x, p.y)
	w.Write(int32(len(s)))
	w.WriteString(s)
}
