package pgproto3_test

import (
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncodeDecode(t *testing.T) {
	srcBytes := []byte{'W', 0x00, 0x00, 0x00, 0x0b, 0x01, 0x00, 0x02, 0x00, 0x00, 0x00, 0x01}
	dstResp := pgproto3.CopyBothResponse{}
	err := dstResp.Decode(srcBytes[5:])
	assert.NoError(t, err, "No errors on decode")
	dstBytes := []byte{}
	dstBytes, err = dstResp.Encode(dstBytes)
	require.NoError(t, err)
	assert.EqualValues(t, srcBytes, dstBytes, "Expecting src & dest bytes to match")
}
