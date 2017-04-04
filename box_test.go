package pgtype_test

import (
	"testing"

	"github.com/jackc/pgx/pgtype"
)

func TestBoxTranscode(t *testing.T) {
	testSuccessfulTranscode(t, "box", []interface{}{
		&pgtype.Box{
			P:      [2]pgtype.Vec2{{7.1, 5.234}, {3.14, 1.678}},
			Status: pgtype.Present,
		},
		&pgtype.Box{
			P:      [2]pgtype.Vec2{{7.1, 1.678}, {-13.14, -5.234}},
			Status: pgtype.Present,
		},
		&pgtype.Box{Status: pgtype.Null},
	})
}

func TestBoxNormalize(t *testing.T) {
	testSuccessfulNormalize(t, []normalizeTest{
		{
			sql: "select '3.14, 1.678, 7.1, 5.234'::box",
			value: &pgtype.Box{
				P:      [2]pgtype.Vec2{{7.1, 5.234}, {3.14, 1.678}},
				Status: pgtype.Present,
			},
		},
	})
}
