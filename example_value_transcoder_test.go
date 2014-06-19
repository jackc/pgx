package pgx_test

import (
	"encoding/binary"
	"fmt"
	"github.com/JackC/pgx"
	"io"
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

func encodePoint(w io.Writer, value interface{}) error {
	p, ok := value.(Point)
	if !ok {
		return fmt.Errorf("Expected Point, received %T", value)
	}

	s := fmt.Sprintf("point(%v,%v)", p.x, p.y)

	err := binary.Write(w, binary.BigEndian, int32(len(s)))
	if err != nil {
		return err
	}

	_, err = io.WriteString(w, s)
	return err
}
