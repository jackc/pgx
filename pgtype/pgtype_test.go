package pgtype_test

import (
	"bytes"
	"database/sql"
	"errors"
	"net"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func mustParseInet(t testing.TB, s string) *net.IPNet {
	ip, ipnet, err := net.ParseCIDR(s)
	if err == nil {
		if ipv4 := ip.To4(); ipv4 != nil {
			ipnet.IP = ipv4
		}
		return ipnet
	}

	// May be bare IP address.
	//
	ip = net.ParseIP(s)
	if ip == nil {
		t.Fatal(errors.New("unable to parse inet address"))
	}
	ipnet = &net.IPNet{IP: ip, Mask: net.CIDRMask(128, 128)}
	if ipv4 := ip.To4(); ipv4 != nil {
		ipnet.IP = ipv4
		ipnet.Mask = net.CIDRMask(32, 32)
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

func TestTypeMapScanNilIsNoOp(t *testing.T) {
	m := pgtype.NewMap()

	err := m.Scan(pgtype.TextOID, pgx.TextFormatCode, []byte("foo"), nil)
	assert.NoError(t, err)
}

func TestTypeMapScanTextFormatInterfacePtr(t *testing.T) {
	m := pgtype.NewMap()
	var got interface{}
	err := m.Scan(pgtype.TextOID, pgx.TextFormatCode, []byte("foo"), &got)
	require.NoError(t, err)
	assert.Equal(t, "foo", got)
}

func TestTypeMapScanTextFormatNonByteaIntoByteSlice(t *testing.T) {
	m := pgtype.NewMap()
	var got []byte
	err := m.Scan(pgtype.JSONBOID, pgx.TextFormatCode, []byte("{}"), &got)
	require.NoError(t, err)
	assert.Equal(t, []byte("{}"), got)
}

func TestTypeMapScanBinaryFormatInterfacePtr(t *testing.T) {
	m := pgtype.NewMap()
	var got interface{}
	err := m.Scan(pgtype.TextOID, pgx.BinaryFormatCode, []byte("foo"), &got)
	require.NoError(t, err)
	assert.Equal(t, "foo", got)
}

func TestTypeMapScanUnknownOIDToStringsAndBytes(t *testing.T) {
	unknownOID := uint32(999999)
	srcBuf := []byte("foo")
	m := pgtype.NewMap()

	var s string
	err := m.Scan(unknownOID, pgx.TextFormatCode, srcBuf, &s)
	assert.NoError(t, err)
	assert.Equal(t, "foo", s)

	var rs _string
	err = m.Scan(unknownOID, pgx.TextFormatCode, srcBuf, &rs)
	assert.NoError(t, err)
	assert.Equal(t, "foo", string(rs))

	var b []byte
	err = m.Scan(unknownOID, pgx.TextFormatCode, srcBuf, &b)
	assert.NoError(t, err)
	assert.Equal(t, []byte("foo"), b)

	var rb _byteSlice
	err = m.Scan(unknownOID, pgx.TextFormatCode, srcBuf, &rb)
	assert.NoError(t, err)
	assert.Equal(t, []byte("foo"), []byte(rb))
}

type pgCustomType struct {
	a string
	b string
}

func (ct *pgCustomType) DecodeText(m *pgtype.Map, buf []byte) error {
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

func TestTypeMapScanUnregisteredOIDToCustomType(t *testing.T) {
	t.Skip("TODO - unskip later in v5") // may no longer be relevent
	unregisteredOID := uint32(999999)
	m := pgtype.NewMap()

	var ct pgCustomType
	err := m.Scan(unregisteredOID, pgx.TextFormatCode, []byte("(foo,bar)"), &ct)
	assert.NoError(t, err)
	assert.Equal(t, "foo", ct.a)
	assert.Equal(t, "bar", ct.b)

	// Scan value into pointer to custom type
	var pCt *pgCustomType
	err = m.Scan(unregisteredOID, pgx.TextFormatCode, []byte("(foo,bar)"), &pCt)
	assert.NoError(t, err)
	require.NotNil(t, pCt)
	assert.Equal(t, "foo", pCt.a)
	assert.Equal(t, "bar", pCt.b)

	// Scan null into pointer to custom type
	err = m.Scan(unregisteredOID, pgx.TextFormatCode, nil, &pCt)
	assert.NoError(t, err)
	assert.Nil(t, pCt)
}

func TestTypeMapScanUnknownOIDTextFormat(t *testing.T) {
	m := pgtype.NewMap()

	var n int32
	err := m.Scan(0, pgx.TextFormatCode, []byte("123"), &n)
	assert.NoError(t, err)
	assert.EqualValues(t, 123, n)
}

func TestTypeMapScanUnknownOIDIntoSQLScanner(t *testing.T) {
	m := pgtype.NewMap()

	var s sql.NullString
	err := m.Scan(0, pgx.TextFormatCode, []byte(nil), &s)
	assert.NoError(t, err)
	assert.Equal(t, "", s.String)
	assert.False(t, s.Valid)
}

func BenchmarkTypeMapScanInt4IntoBinaryDecoder(b *testing.B) {
	m := pgtype.NewMap()
	src := []byte{0, 0, 0, 42}
	var v pgtype.Int4

	for i := 0; i < b.N; i++ {
		v = pgtype.Int4{}
		err := m.Scan(pgtype.Int4OID, pgtype.BinaryFormatCode, src, &v)
		if err != nil {
			b.Fatal(err)
		}
		if v != (pgtype.Int4{Int: 42, Valid: true}) {
			b.Fatal("scan failed due to bad value")
		}
	}
}

func BenchmarkTypeMapScanInt4IntoGoInt32(b *testing.B) {
	m := pgtype.NewMap()
	src := []byte{0, 0, 0, 42}
	var v int32

	for i := 0; i < b.N; i++ {
		v = 0
		err := m.Scan(pgtype.Int4OID, pgtype.BinaryFormatCode, src, &v)
		if err != nil {
			b.Fatal(err)
		}
		if v != 42 {
			b.Fatal("scan failed due to bad value")
		}
	}
}

func BenchmarkScanPlanScanInt4IntoBinaryDecoder(b *testing.B) {
	m := pgtype.NewMap()
	src := []byte{0, 0, 0, 42}
	var v pgtype.Int4

	plan := m.PlanScan(pgtype.Int4OID, pgtype.BinaryFormatCode, &v)

	for i := 0; i < b.N; i++ {
		v = pgtype.Int4{}
		err := plan.Scan(src, &v)
		if err != nil {
			b.Fatal(err)
		}
		if v != (pgtype.Int4{Int: 42, Valid: true}) {
			b.Fatal("scan failed due to bad value")
		}
	}
}

func BenchmarkScanPlanScanInt4IntoGoInt32(b *testing.B) {
	m := pgtype.NewMap()
	src := []byte{0, 0, 0, 42}
	var v int32

	plan := m.PlanScan(pgtype.Int4OID, pgtype.BinaryFormatCode, &v)

	for i := 0; i < b.N; i++ {
		v = 0
		err := plan.Scan(src, &v)
		if err != nil {
			b.Fatal(err)
		}
		if v != 42 {
			b.Fatal("scan failed due to bad value")
		}
	}
}

func isExpectedEq(a interface{}) func(interface{}) bool {
	return func(v interface{}) bool {
		return a == v
	}
}
