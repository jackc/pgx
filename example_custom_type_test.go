package pgx_test

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

var pointRegexp *regexp.Regexp = regexp.MustCompile(`^\((.*),(.*)\)$`)

// Point represents a point that may be null.
type Point struct {
	X, Y  float64 // Coordinates of point
	Valid bool
}

func (dst *Point) Set(src interface{}) error {
	return fmt.Errorf("cannot convert %v to Point", src)
}

func (dst *Point) Get() interface{} {
	if !dst.Valid {
		return nil
	}

	return dst
}

func (src *Point) AssignTo(dst interface{}) error {
	return fmt.Errorf("cannot assign %v to %T", src, dst)
}

func (dst *Point) DecodeText(ci *pgtype.ConnInfo, src []byte) error {
	if src == nil {
		*dst = Point{}
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

	*dst = Point{X: x, Y: y, Valid: true}

	return nil
}

func (src *Point) String() string {
	if !src.Valid {
		return "null point"
	}

	return fmt.Sprintf("%.1f, %.1f", src.X, src.Y)
}

func Example_CustomType() {
	conn, err := pgx.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	if err != nil {
		fmt.Printf("Unable to establish connection: %v", err)
		return
	}
	defer conn.Close(context.Background())

	if conn.PgConn().ParameterStatus("crdb_version") != "" {
		// Skip test / example when running on CockroachDB which doesn't support the point type. Since an example can't be
		// skipped fake success instead.
		fmt.Println("null point")
		fmt.Println("1.5, 2.5")
		return
	}

	// Override registered handler for point
	conn.ConnInfo().RegisterDataType(pgtype.DataType{
		Value: &Point{},
		Name:  "point",
		OID:   600,
	})

	p := &Point{}
	err = conn.QueryRow(context.Background(), "select null::point").Scan(p)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(p)

	err = conn.QueryRow(context.Background(), "select point(1.5,2.5)").Scan(p)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(p)
	// Output:
	// null point
	// 1.5, 2.5
}
