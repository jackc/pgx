package pgtype_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxtest"
)

func isExpectedEqPolygon(a any) func(any) bool {
	return func(v any) bool {
		ap := a.(pgtype.Polygon)
		vp := v.(pgtype.Polygon)

		if !(ap.Valid == vp.Valid && len(ap.P) == len(vp.P)) {
			return false
		}

		for i := range ap.P {
			if ap.P[i] != vp.P[i] {
				return false
			}
		}

		return true
	}
}

func TestPolygonTranscode(t *testing.T) {
	skipCockroachDB(t, "Server does not support type polygon")

	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "polygon", []pgxtest.ValueRoundTripTest{
		{
			pgtype.Polygon{
				P:     []pgtype.Vec2{{3.14, 1.678901234}, {7.1, 5.234}, {5.0, 3.234}},
				Valid: true,
			},
			new(pgtype.Polygon),
			isExpectedEqPolygon(pgtype.Polygon{
				P:     []pgtype.Vec2{{3.14, 1.678901234}, {7.1, 5.234}, {5.0, 3.234}},
				Valid: true,
			}),
		},
		{
			pgtype.Polygon{
				P:     []pgtype.Vec2{{3.14, -1.678}, {7.1, -5.234}, {23.1, 9.34}},
				Valid: true,
			},
			new(pgtype.Polygon),
			isExpectedEqPolygon(pgtype.Polygon{
				P:     []pgtype.Vec2{{3.14, -1.678}, {7.1, -5.234}, {23.1, 9.34}},
				Valid: true,
			}),
		},
		{pgtype.Polygon{}, new(pgtype.Polygon), isExpectedEqPolygon(pgtype.Polygon{})},
		{nil, new(pgtype.Polygon), isExpectedEqPolygon(pgtype.Polygon{})},
	})
}
