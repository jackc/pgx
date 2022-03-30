package pgproto3_test

import (
	"testing"

	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/assert"
)

func TestEncodeDecode(t *testing.T) {
	srcBytes := []byte{'W', 0x00, 0x00, 0x00, 0x0b, 0x01, 0x00, 0x02, 0x00, 0x00, 0x00, 0x01}
	dstResp := pgproto3.CopyBothResponse{}
	err := dstResp.Decode(srcBytes[5:])
	assert.NoError(t, err, "No errors on decode")
	dstBytes := []byte{}
	dstBytes = dstResp.Encode(dstBytes)
	assert.EqualValues(t, srcBytes, dstBytes, "Expecting src & dest bytes to match")
}
