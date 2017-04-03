package pgtype_test

import (
	"testing"

	"github.com/jackc/pgx/pgtype"
)

func TestBoxTranscode(t *testing.T) {
	testSuccessfulTranscode(t, "box", []interface{}{
		&pgtype.Box{
			Corners: []Vec2{{3.14, 1.678}, {7.1, 5.234}},
			Status:  pgtype.Present,
		},
		&pgtype.Box{
			Corners: []Vec2{{-13.14, 1.678}, {7.1, -5.234}},
			Status:  pgtype.Present,
		},
		&pgtype.Box{Status: pgtype.Null},
	})
}
