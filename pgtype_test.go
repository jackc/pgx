package pgtype_test

import (
	"bytes"
	"net"
	"testing"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	_ "github.com/jackc/pgx/v4/stdlib"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	errors "golang.org/x/xerrors"
)

// Test for renamed types
type _string string
type _bool bool
type _int8 int8
type _int16 int16
type _int16Slice []int16
type _int32Slice []int32
type _int64Slice []int64
type _float32Slice []float32
type _float64Slice []float64
type _byteSlice []byte

func mustParseCIDR(t testing.TB, s string) *net.IPNet {
	_, ipnet, err := net.ParseCIDR(s)
	if err != nil {
		t.Fatal(err)
	}

	return ipnet
}

func mustParseMacaddr(t testing.TB, s string) net.HardwareAddr {
	addr, err := net.ParseMAC(s)
	if err != nil {
		t.Fatal(err)
	}

	return addr
}

func TestConnInfoScanUnknownOIDToStringsAndBytes(t *testing.T) {
	unknownOID := uint32(999999)
	srcBuf := []byte("foo")
	ci := pgtype.NewConnInfo()

	var s string
	err := ci.Scan(unknownOID, pgx.TextFormatCode, srcBuf, &s)
	assert.NoError(t, err)
	assert.Equal(t, "foo", s)

	var rs _string
	err = ci.Scan(unknownOID, pgx.TextFormatCode, srcBuf, &rs)
	assert.NoError(t, err)
	assert.Equal(t, "foo", string(rs))

	var b []byte
	err = ci.Scan(unknownOID, pgx.TextFormatCode, srcBuf, &b)
	assert.NoError(t, err)
	assert.Equal(t, []byte("foo"), b)

	err = ci.Scan(unknownOID, pgx.BinaryFormatCode, srcBuf, &b)
	assert.NoError(t, err)
	assert.Equal(t, []byte("foo"), b)

	var rb _byteSlice
	err = ci.Scan(unknownOID, pgx.TextFormatCode, srcBuf, &rb)
	assert.NoError(t, err)
	assert.Equal(t, []byte("foo"), []byte(rb))

	err = ci.Scan(unknownOID, pgx.BinaryFormatCode, srcBuf, &b)
	assert.NoError(t, err)
	assert.Equal(t, []byte("foo"), []byte(rb))
}

type pgCustomType struct {
	a string
	b string
}

func (ct *pgCustomType) DecodeText(ci *pgtype.ConnInfo, buf []byte) error {
	// This is not a complete parser for the text format of composite types. This is just for test purposes.
	if buf == nil {
		return errors.New("cannot parse null")
	}

	if len(buf) < 2 {
		return errors.New("invalid text format")
	}

	parts := bytes.Split(buf[1:len(buf)-1], []byte(","))
	if len(parts) != 2 {
		return errors.New("wrong number of parts")
	}

	ct.a = string(parts[0])
	ct.b = string(parts[1])

	return nil
}

func TestConnInfoScanUnknownOIDToCustomType(t *testing.T) {
	unknownOID := uint32(999999)
	ci := pgtype.NewConnInfo()

	var ct pgCustomType
	err := ci.Scan(unknownOID, pgx.TextFormatCode, []byte("(foo,bar)"), &ct)
	assert.NoError(t, err)
	assert.Equal(t, "foo", ct.a)
	assert.Equal(t, "bar", ct.b)

	// Scan value into pointer to custom type
	var pCt *pgCustomType
	err = ci.Scan(unknownOID, pgx.TextFormatCode, []byte("(foo,bar)"), &pCt)
	assert.NoError(t, err)
	require.NotNil(t, pCt)
	assert.Equal(t, "foo", pCt.a)
	assert.Equal(t, "bar", pCt.b)

	// Scan null into pointer to custom type
	err = ci.Scan(unknownOID, pgx.TextFormatCode, nil, &pCt)
	assert.NoError(t, err)
	assert.Nil(t, pCt)
}
