package pgtype_test

import (
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgtype/testutil"
)

func TestLsegTranscode(t *testing.T) {
	skipCockroachDB(t, "Server does not support type lseg")

	testutil.RunTranscodeTests(t, "lseg", []testutil.TranscodeTestCase{
		{
			pgtype.Lseg{
				P:     [2]pgtype.Vec2{{3.14, 1.678}, {7.1, 5.2345678901}},
				Valid: true,
			},
			new(pgtype.Lseg),
			isExpectedEq(pgtype.Lseg{
				P:     [2]pgtype.Vec2{{3.14, 1.678}, {7.1, 5.2345678901}},
				Valid: true,
			}),
		},
		{
			pgtype.Lseg{
				P:     [2]pgtype.Vec2{{7.1, 1.678}, {-13.14, -5.234}},
				Valid: true,
			},
			new(pgtype.Lseg),
			isExpectedEq(pgtype.Lseg{
				P:     [2]pgtype.Vec2{{7.1, 1.678}, {-13.14, -5.234}},
				Valid: true,
			}),
		},
		{pgtype.Lseg{}, new(pgtype.Lseg), isExpectedEq(pgtype.Lseg{})},
		{nil, new(pgtype.Lseg), isExpectedEq(pgtype.Lseg{})},
	})
}
