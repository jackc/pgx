package pgtype_test

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// Point represents a point that may be null.
type Point struct {
	X, Y  float32 // Coordinates of point
	Valid bool
}

func (p *Point) ScanPoint(v pgtype.Point) error {
	*p = Point{
		X:     float32(v.P.X),
		Y:     float32(v.P.Y),
		Valid: v.Valid,
	}
	return nil
}

func (p Point) PointValue() (pgtype.Point, error) {
	return pgtype.Point{
		P:     pgtype.Vec2{X: float64(p.X), Y: float64(p.Y)},
		Valid: true,
	}, nil
}

func (src *Point) String() string {
	if !src.Valid {
		return "null point"
	}

	return fmt.Sprintf("%.1f, %.1f", src.X, src.Y)
}

func Example_customType() {
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
