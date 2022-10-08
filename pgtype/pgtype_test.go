package pgtype_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"os"
	"regexp"
	"strconv"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxtest"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var defaultConnTestRunner pgxtest.ConnTestRunner

func init() {
	defaultConnTestRunner = pgxtest.DefaultConnTestRunner()
	defaultConnTestRunner.CreateConfig = func(ctx context.Context, t testing.TB) *pgx.ConnConfig {
		config, err := pgx.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
		require.NoError(t, err)
		return config
	}
}

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
		} else {
			ipnet.IP = ip
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

func skipCockroachDB(t testing.TB, msg string) {
	conn, err := pgx.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())

	if conn.PgConn().ParameterStatus("crdb_version") != "" {
		t.Skip(msg)
	}
}

func skipPostgreSQLVersionLessThan(t testing.TB, minVersion int64) {
	conn, err := pgx.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())

	serverVersionStr := conn.PgConn().ParameterStatus("server_version")
	serverVersionStr = regexp.MustCompile(`^[0-9]+`).FindString(serverVersionStr)
	// if not PostgreSQL do nothing
	if serverVersionStr == "" {
		return
	}

	serverVersion, err := strconv.ParseInt(serverVersionStr, 10, 64)
	require.NoError(t, err)

	if serverVersion < minVersion {
		t.Skipf("Test requires PostgreSQL v%d+", minVersion)
	}
}

func TestMapScanNilIsNoOp(t *testing.T) {
	m := pgtype.NewMap()

	err := m.Scan(pgtype.TextOID, pgx.TextFormatCode, []byte("foo"), nil)
	assert.NoError(t, err)
}

func TestMapScanTextFormatInterfacePtr(t *testing.T) {
	m := pgtype.NewMap()
	var got any
	err := m.Scan(pgtype.TextOID, pgx.TextFormatCode, []byte("foo"), &got)
	require.NoError(t, err)
	assert.Equal(t, "foo", got)
}

func TestMapScanTextFormatNonByteaIntoByteSlice(t *testing.T) {
	m := pgtype.NewMap()
	var got []byte
	err := m.Scan(pgtype.JSONBOID, pgx.TextFormatCode, []byte("{}"), &got)
	require.NoError(t, err)
	assert.Equal(t, []byte("{}"), got)
}

func TestMapScanBinaryFormatInterfacePtr(t *testing.T) {
	m := pgtype.NewMap()
	var got any
	err := m.Scan(pgtype.TextOID, pgx.BinaryFormatCode, []byte("foo"), &got)
	require.NoError(t, err)
	assert.Equal(t, "foo", got)
}

func TestMapScanUnknownOIDToStringsAndBytes(t *testing.T) {
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

func TestMapScanPointerToNilStructDoesNotCrash(t *testing.T) {
	m := pgtype.NewMap()

	type myStruct struct{}
	var p *myStruct
	err := m.Scan(0, pgx.TextFormatCode, []byte("(foo,bar)"), &p)
	require.NotNil(t, err)
}

func TestMapScanUnknownOIDTextFormat(t *testing.T) {
	m := pgtype.NewMap()

	var n int32
	err := m.Scan(0, pgx.TextFormatCode, []byte("123"), &n)
	assert.NoError(t, err)
	assert.EqualValues(t, 123, n)
}

func TestMapScanUnknownOIDIntoSQLScanner(t *testing.T) {
	m := pgtype.NewMap()

	var s sql.NullString
	err := m.Scan(0, pgx.TextFormatCode, []byte(nil), &s)
	assert.NoError(t, err)
	assert.Equal(t, "", s.String)
	assert.False(t, s.Valid)
}

type pgCustomInt int64

func (ci *pgCustomInt) Scan(src interface{}) error {
	*ci = pgCustomInt(src.(int64))
	return nil
}

func TestScanPlanBinaryInt32ScanScanner(t *testing.T) {
	m := pgtype.NewMap()
	src := []byte{0, 42}
	var v pgCustomInt

	plan := m.PlanScan(pgtype.Int2OID, pgtype.BinaryFormatCode, &v)
	err := plan.Scan(src, &v)
	require.NoError(t, err)
	require.EqualValues(t, 42, v)

	ptr := new(pgCustomInt)
	plan = m.PlanScan(pgtype.Int2OID, pgtype.BinaryFormatCode, &ptr)
	err = plan.Scan(src, &ptr)
	require.NoError(t, err)
	require.EqualValues(t, 42, *ptr)

	ptr = new(pgCustomInt)
	err = plan.Scan(nil, &ptr)
	require.NoError(t, err)
	assert.Nil(t, ptr)

	ptr = nil
	plan = m.PlanScan(pgtype.Int2OID, pgtype.BinaryFormatCode, &ptr)
	err = plan.Scan(src, &ptr)
	require.NoError(t, err)
	require.EqualValues(t, 42, *ptr)

	ptr = nil
	plan = m.PlanScan(pgtype.Int2OID, pgtype.BinaryFormatCode, &ptr)
	err = plan.Scan(nil, &ptr)
	require.NoError(t, err)
	assert.Nil(t, ptr)
}

// Test for https://github.com/jackc/pgtype/issues/164
func TestScanPlanInterface(t *testing.T) {
	m := pgtype.NewMap()
	src := []byte{0, 42}
	var v interface{}
	plan := m.PlanScan(pgtype.Int2OID, pgtype.BinaryFormatCode, v)
	err := plan.Scan(src, v)
	assert.Error(t, err)
}

// https://github.com/jackc/pgx/issues/1263
func TestMapScanPtrToPtrToSlice(t *testing.T) {
	m := pgtype.NewMap()
	src := []byte("{foo,bar}")
	var v *[]string
	plan := m.PlanScan(pgtype.TextArrayOID, pgtype.TextFormatCode, &v)
	err := plan.Scan(src, &v)
	require.NoError(t, err)
	require.Equal(t, []string{"foo", "bar"}, *v)
}

type databaseValuerString string

func (s databaseValuerString) Value() (driver.Value, error) {
	return fmt.Sprintf("%d", len(s)), nil
}

// https://github.com/jackc/pgx/issues/1319
func TestMapEncodeTextFormatDatabaseValuerThatIsRenamedSimpleType(t *testing.T) {
	m := pgtype.NewMap()
	src := databaseValuerString("foo")
	buf, err := m.Encode(pgtype.TextOID, pgtype.TextFormatCode, src, nil)
	require.NoError(t, err)
	require.Equal(t, "3", string(buf))
}

type databaseValuerFmtStringer string

func (s databaseValuerFmtStringer) Value() (driver.Value, error) {
	return nil, nil
}

func (s databaseValuerFmtStringer) String() string {
	return "foobar"
}

// https://github.com/jackc/pgx/issues/1311
func TestMapEncodeTextFormatDatabaseValuerThatIsFmtStringer(t *testing.T) {
	m := pgtype.NewMap()
	src := databaseValuerFmtStringer("")
	buf, err := m.Encode(pgtype.TextOID, pgtype.TextFormatCode, src, nil)
	require.NoError(t, err)
	require.Nil(t, buf)
}

type databaseValuerStringFormat struct {
	n int32
}

func (v databaseValuerStringFormat) Value() (driver.Value, error) {
	return fmt.Sprint(v.n), nil
}

func TestMapEncodeBinaryFormatDatabaseValuerThatReturnsString(t *testing.T) {
	m := pgtype.NewMap()
	src := databaseValuerStringFormat{n: 42}
	buf, err := m.Encode(pgtype.Int4OID, pgtype.BinaryFormatCode, src, nil)
	require.NoError(t, err)
	require.Equal(t, []byte{0, 0, 0, 42}, buf)
}

// https://github.com/jackc/pgx/issues/1326
func TestMapScanPointerToRenamedType(t *testing.T) {
	srcBuf := []byte("foo")
	m := pgtype.NewMap()

	var rs *_string
	err := m.Scan(pgtype.TextOID, pgx.TextFormatCode, srcBuf, &rs)
	assert.NoError(t, err)
	require.NotNil(t, rs)
	assert.Equal(t, "foo", string(*rs))
}

// https://github.com/jackc/pgx/issues/1326
func TestMapScanNullToWrongType(t *testing.T) {
	m := pgtype.NewMap()

	var n *int32
	err := m.Scan(pgtype.TextOID, pgx.TextFormatCode, nil, &n)
	assert.NoError(t, err)
	assert.Nil(t, n)

	var pn pgtype.Int4
	err = m.Scan(pgtype.TextOID, pgx.TextFormatCode, nil, &pn)
	assert.NoError(t, err)
	assert.False(t, pn.Valid)
}

func BenchmarkMapScanInt4IntoBinaryDecoder(b *testing.B) {
	m := pgtype.NewMap()
	src := []byte{0, 0, 0, 42}
	var v pgtype.Int4

	for i := 0; i < b.N; i++ {
		v = pgtype.Int4{}
		err := m.Scan(pgtype.Int4OID, pgtype.BinaryFormatCode, src, &v)
		if err != nil {
			b.Fatal(err)
		}
		if v != (pgtype.Int4{Int32: 42, Valid: true}) {
			b.Fatal("scan failed due to bad value")
		}
	}
}

func BenchmarkMapScanInt4IntoGoInt32(b *testing.B) {
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
		if v != (pgtype.Int4{Int32: 42, Valid: true}) {
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

func isExpectedEq(a any) func(any) bool {
	return func(v any) bool {
		return a == v
	}
}
