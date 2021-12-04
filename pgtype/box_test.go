package pgtype_test

import (
	"testing"

	"github.com/jackc/pgx/v4/pgtype"
	"github.com/jackc/pgx/v4/pgtype/testutil"
)

func TestBoxTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscode(t, "box", []interface{}{
		&pgtype.Box{
			P:     [2]pgtype.Vec2{{7.1, 5.2345678}, {3.14, 1.678}},
			Valid: true,
		},
		&pgtype.Box{
			P:     [2]pgtype.Vec2{{7.1, 1.678}, {-13.14, -5.234}},
			Valid: true,
		},
		&pgtype.Box{},
	})
}

func TestBoxNormalize(t *testing.T) {
	testutil.TestSuccessfulNormalize(t, []testutil.NormalizeTest{
		{
			SQL: "select '3.14, 1.678, 7.1, 5.234'::box",
			Value: &pgtype.Box{
				P:     [2]pgtype.Vec2{{7.1, 5.234}, {3.14, 1.678}},
				Valid: true,
			},
		},
	})
}
