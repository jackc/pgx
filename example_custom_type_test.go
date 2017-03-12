package pgx_test

import (
	"fmt"
	"io"
	"regexp"
	"strconv"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"
)

var pointRegexp *regexp.Regexp = regexp.MustCompile(`^\((.*),(.*)\)$`)

// Point represents a point that may be null.
type Point struct {
	X, Y   float64 // Coordinates of point
	Status pgtype.Status
}

func (dst *Point) DecodeText(src []byte) error {
	if src == nil {
		*dst = Point{Status: pgtype.Null}
		return nil
	}

	s := string(src)
	match := pointRegexp.FindStringSubmatch(s)
	if match == nil {
		return fmt.Errorf("Received invalid point: %v", s)
	}

	x, err := strconv.ParseFloat(match[1], 64)
	if err != nil {
		return fmt.Errorf("Received invalid point: %v", s)
	}
	y, err := strconv.ParseFloat(match[2], 64)
	if err != nil {
		return fmt.Errorf("Received invalid point: %v", s)
	}

	*dst = Point{X: x, Y: y, Status: pgtype.Present}

	return nil
}

func (src Point) EncodeText(w io.Writer) (bool, error) {
	switch src.Status {
	case pgtype.Null:
		return true, nil
	case pgtype.Undefined:
		return false, fmt.Errorf("undefined")
	}

	_, err := io.WriteString(w, fmt.Sprintf("point(%v,%v)", src.X, src.Y))
	return false, err
}

func (p Point) String() string {
	if p.Status == pgtype.Present {
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

	var p Point
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
