package pgtype_test

import (
	"encoding/binary"
	"testing"

	"github.com/jackc/pgx/v5/internal/pgio"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
)

// The binary numeric digit loop ran (ndigits+3)/4 times, computed in the
// uint16 ndigits was read into. A count above 65532 wrapped that addition to a
// small number, so the loop ran too few times or not at all and the digits
// were silently dropped rather than reported as an error.
func TestNumericBinaryDecodeLargeNDigits(t *testing.T) {
	const ndigits = 65533 // ndigits+3 overflows uint16

	buf := make([]byte, 0, 8+ndigits*2)
	buf = pgio.AppendUint16(buf, ndigits)
	buf = pgio.AppendInt16(buf, 0)  // weight
	buf = pgio.AppendUint16(buf, 0) // sign: positive
	buf = pgio.AppendInt16(buf, 0)  // dscale

	// All digits zero except the least significant, so the accumulated value is
	// exactly 1 and the big.Int multiplications stay cheap.
	buf = append(buf, make([]byte, ndigits*2)...)
	binary.BigEndian.PutUint16(buf[len(buf)-2:], 1)

	m := pgtype.NewMap()
	var n pgtype.Numeric
	plan := m.PlanScan(pgtype.NumericOID, pgtype.BinaryFormatCode, &n)
	require.NotNil(t, plan)
	require.NoError(t, plan.Scan(buf, &n))

	require.True(t, n.Valid)
	require.NotNil(t, n.Int)
	// Before the fix the digits were skipped entirely and this was 0.
	require.Equal(t, int64(1), n.Int.Int64())
	require.Equal(t, int32((0-ndigits+1)*4), n.Exp)
}

// Trailing bytes after the digits a numeric declares are malformed.
func TestNumericBinaryDecodeTrailingBytes(t *testing.T) {
	buf := make([]byte, 0, 12)
	buf = pgio.AppendUint16(buf, 1) // ndigits
	buf = pgio.AppendInt16(buf, 0)  // weight
	buf = pgio.AppendUint16(buf, 0) // sign: positive
	buf = pgio.AppendInt16(buf, 0)  // dscale
	buf = pgio.AppendUint16(buf, 1) // the single digit
	buf = pgio.AppendUint16(buf, 0) // trailing

	m := pgtype.NewMap()
	var n pgtype.Numeric
	plan := m.PlanScan(pgtype.NumericOID, pgtype.BinaryFormatCode, &n)
	require.NotNil(t, plan)
	require.Error(t, plan.Scan(buf, &n))
}
