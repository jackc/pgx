package pgx_test

import (
	"bytes"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx"
)

func TestDateTranscode(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	dates := []time.Time{
		time.Date(1, 1, 1, 0, 0, 0, 0, time.Local),
		time.Date(1000, 1, 1, 0, 0, 0, 0, time.Local),
		time.Date(1600, 1, 1, 0, 0, 0, 0, time.Local),
		time.Date(1700, 1, 1, 0, 0, 0, 0, time.Local),
		time.Date(1800, 1, 1, 0, 0, 0, 0, time.Local),
		time.Date(1900, 1, 1, 0, 0, 0, 0, time.Local),
		time.Date(1990, 1, 1, 0, 0, 0, 0, time.Local),
		time.Date(1999, 12, 31, 0, 0, 0, 0, time.Local),
		time.Date(2000, 1, 1, 0, 0, 0, 0, time.Local),
		time.Date(2001, 1, 2, 0, 0, 0, 0, time.Local),
		time.Date(2004, 2, 29, 0, 0, 0, 0, time.Local),
		time.Date(2013, 7, 4, 0, 0, 0, 0, time.Local),
		time.Date(2013, 12, 25, 0, 0, 0, 0, time.Local),
		time.Date(2029, 1, 1, 0, 0, 0, 0, time.Local),
		time.Date(2081, 1, 1, 0, 0, 0, 0, time.Local),
		time.Date(2096, 2, 29, 0, 0, 0, 0, time.Local),
		time.Date(2550, 1, 1, 0, 0, 0, 0, time.Local),
		time.Date(9999, 12, 31, 0, 0, 0, 0, time.Local),
	}

	for _, actualDate := range dates {
		var d time.Time

		err := conn.QueryRow("select $1::date", actualDate).Scan(&d)
		if err != nil {
			t.Fatalf("Unexpected failure on QueryRow Scan: %v", err)
		}
		if !actualDate.Equal(d) {
			t.Errorf("Did not transcode date successfully: %v is not %v", d, actualDate)
		}
	}
}

func TestTimestampTzTranscode(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	inputTime := time.Date(2013, 1, 2, 3, 4, 5, 6000, time.Local)

	var outputTime time.Time

	err := conn.QueryRow("select $1::timestamptz", inputTime).Scan(&outputTime)
	if err != nil {
		t.Fatalf("QueryRow Scan failed: %v", err)
	}
	if !inputTime.Equal(outputTime) {
		t.Errorf("Did not transcode time successfully: %v is not %v", outputTime, inputTime)
	}

	err = conn.QueryRow("select $1::timestamptz", inputTime).Scan(&outputTime)
	if err != nil {
		t.Fatalf("QueryRow Scan failed: %v", err)
	}
	if !inputTime.Equal(outputTime) {
		t.Errorf("Did not transcode time successfully: %v is not %v", outputTime, inputTime)
	}
}

func TestJsonAndJsonbTranscode(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	for _, oid := range []pgx.Oid{pgx.JsonOid, pgx.JsonbOid} {
		if _, ok := conn.PgTypes[oid]; !ok {
			return // No JSON/JSONB type -- must be running against old PostgreSQL
		}

		for _, format := range []int16{pgx.TextFormatCode, pgx.BinaryFormatCode} {
			pgtype := conn.PgTypes[oid]
			pgtype.DefaultFormat = format
			conn.PgTypes[oid] = pgtype

			typename := conn.PgTypes[oid].Name

			testJsonString(t, conn, typename, format)
			testJsonStringPointer(t, conn, typename, format)
			testJsonSingleLevelStringMap(t, conn, typename, format)
			testJsonNestedMap(t, conn, typename, format)
			testJsonStringArray(t, conn, typename, format)
			testJsonInt64Array(t, conn, typename, format)
			testJsonInt16ArrayFailureDueToOverflow(t, conn, typename, format)
			testJsonStruct(t, conn, typename, format)
		}
	}
}

func testJsonString(t *testing.T, conn *pgx.Conn, typename string, format int16) {
	input := `{"key": "value"}`
	expectedOutput := map[string]string{"key": "value"}
	var output map[string]string
	err := conn.QueryRow("select $1::"+typename, input).Scan(&output)
	if err != nil {
		t.Errorf("%s %d: QueryRow Scan failed: %v", typename, format, err)
		return
	}

	if !reflect.DeepEqual(expectedOutput, output) {
		t.Errorf("%s %d: Did not transcode map[string]string successfully: %v is not %v", typename, format, expectedOutput, output)
		return
	}
}

func testJsonStringPointer(t *testing.T, conn *pgx.Conn, typename string, format int16) {
	input := `{"key": "value"}`
	expectedOutput := map[string]string{"key": "value"}
	var output map[string]string
	err := conn.QueryRow("select $1::"+typename, &input).Scan(&output)
	if err != nil {
		t.Errorf("%s %d: QueryRow Scan failed: %v", typename, format, err)
		return
	}

	if !reflect.DeepEqual(expectedOutput, output) {
		t.Errorf("%s %d: Did not transcode map[string]string successfully: %v is not %v", typename, format, expectedOutput, output)
		return
	}
}

func testJsonSingleLevelStringMap(t *testing.T, conn *pgx.Conn, typename string, format int16) {
	input := map[string]string{"key": "value"}
	var output map[string]string
	err := conn.QueryRow("select $1::"+typename, input).Scan(&output)
	if err != nil {
		t.Errorf("%s %d: QueryRow Scan failed: %v", typename, format, err)
		return
	}

	if !reflect.DeepEqual(input, output) {
		t.Errorf("%s %d: Did not transcode map[string]string successfully: %v is not %v", typename, format, input, output)
		return
	}
}

func testJsonNestedMap(t *testing.T, conn *pgx.Conn, typename string, format int16) {
	input := map[string]interface{}{
		"name":      "Uncanny",
		"stats":     map[string]interface{}{"hp": float64(107), "maxhp": float64(150)},
		"inventory": []interface{}{"phone", "key"},
	}
	var output map[string]interface{}
	err := conn.QueryRow("select $1::"+typename, input).Scan(&output)
	if err != nil {
		t.Errorf("%s %d: QueryRow Scan failed: %v", typename, format, err)
		return
	}

	if !reflect.DeepEqual(input, output) {
		t.Errorf("%s %d: Did not transcode map[string]interface{} successfully: %v is not %v", typename, format, input, output)
		return
	}
}

func testJsonStringArray(t *testing.T, conn *pgx.Conn, typename string, format int16) {
	input := []string{"foo", "bar", "baz"}
	var output []string
	err := conn.QueryRow("select $1::"+typename, input).Scan(&output)
	if err != nil {
		t.Errorf("%s %d: QueryRow Scan failed: %v", typename, format, err)
	}

	if !reflect.DeepEqual(input, output) {
		t.Errorf("%s %d: Did not transcode []string successfully: %v is not %v", typename, format, input, output)
	}
}

func testJsonInt64Array(t *testing.T, conn *pgx.Conn, typename string, format int16) {
	input := []int64{1, 2, 234432}
	var output []int64
	err := conn.QueryRow("select $1::"+typename, input).Scan(&output)
	if err != nil {
		t.Errorf("%s %d: QueryRow Scan failed: %v", typename, format, err)
	}

	if !reflect.DeepEqual(input, output) {
		t.Errorf("%s %d: Did not transcode []int64 successfully: %v is not %v", typename, format, input, output)
	}
}

func testJsonInt16ArrayFailureDueToOverflow(t *testing.T, conn *pgx.Conn, typename string, format int16) {
	input := []int{1, 2, 234432}
	var output []int16
	err := conn.QueryRow("select $1::"+typename, input).Scan(&output)
	if err == nil || err.Error() != "can't scan into dest[0]: json: cannot unmarshal number 234432 into Go value of type int16" {
		t.Errorf("%s %d: Expected *json.UnmarkalTypeError, but got %v", typename, format, err)
	}
}

func testJsonStruct(t *testing.T, conn *pgx.Conn, typename string, format int16) {
	type person struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}

	input := person{
		Name: "John",
		Age:  42,
	}

	var output person

	err := conn.QueryRow("select $1::"+typename, input).Scan(&output)
	if err != nil {
		t.Errorf("%s %d: QueryRow Scan failed: %v", typename, format, err)
	}

	if !reflect.DeepEqual(input, output) {
		t.Errorf("%s %d: Did not transcode struct successfully: %v is not %v", typename, format, input, output)
	}
}

func mustParseCIDR(t *testing.T, s string) net.IPNet {
	_, ipnet, err := net.ParseCIDR(s)
	if err != nil {
		t.Fatal(err)
	}

	return *ipnet
}

func TestStringToNotTextTypeTranscode(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	input := "01086ee0-4963-4e35-9116-30c173a8d0bd"

	var output string
	err := conn.QueryRow("select $1::uuid", input).Scan(&output)
	if err != nil {
		t.Fatal(err)
	}
	if input != output {
		t.Errorf("uuid: Did not transcode string successfully: %s is not %s", input, output)
	}

	err = conn.QueryRow("select $1::uuid", &input).Scan(&output)
	if err != nil {
		t.Fatal(err)
	}
	if input != output {
		t.Errorf("uuid: Did not transcode pointer to string successfully: %s is not %s", input, output)
	}
}

func TestInetCidrTranscodeIPNet(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	tests := []struct {
		sql   string
		value net.IPNet
	}{
		{"select $1::inet", mustParseCIDR(t, "0.0.0.0/32")},
		{"select $1::inet", mustParseCIDR(t, "127.0.0.1/32")},
		{"select $1::inet", mustParseCIDR(t, "12.34.56.0/32")},
		{"select $1::inet", mustParseCIDR(t, "192.168.1.0/24")},
		{"select $1::inet", mustParseCIDR(t, "255.0.0.0/8")},
		{"select $1::inet", mustParseCIDR(t, "255.255.255.255/32")},
		{"select $1::inet", mustParseCIDR(t, "::/128")},
		{"select $1::inet", mustParseCIDR(t, "::/0")},
		{"select $1::inet", mustParseCIDR(t, "::1/128")},
		{"select $1::inet", mustParseCIDR(t, "2607:f8b0:4009:80b::200e/128")},
		{"select $1::cidr", mustParseCIDR(t, "0.0.0.0/32")},
		{"select $1::cidr", mustParseCIDR(t, "127.0.0.1/32")},
		{"select $1::cidr", mustParseCIDR(t, "12.34.56.0/32")},
		{"select $1::cidr", mustParseCIDR(t, "192.168.1.0/24")},
		{"select $1::cidr", mustParseCIDR(t, "255.0.0.0/8")},
		{"select $1::cidr", mustParseCIDR(t, "255.255.255.255/32")},
		{"select $1::cidr", mustParseCIDR(t, "::/128")},
		{"select $1::cidr", mustParseCIDR(t, "::/0")},
		{"select $1::cidr", mustParseCIDR(t, "::1/128")},
		{"select $1::cidr", mustParseCIDR(t, "2607:f8b0:4009:80b::200e/128")},
	}

	for i, tt := range tests {
		var actual net.IPNet

		err := conn.QueryRow(tt.sql, tt.value).Scan(&actual)
		if err != nil {
			t.Errorf("%d. Unexpected failure: %v (sql -> %v, value -> %v)", i, err, tt.sql, tt.value)
			continue
		}

		if actual.String() != tt.value.String() {
			t.Errorf("%d. Expected %v, got %v (sql -> %v)", i, tt.value, actual, tt.sql)
		}

		ensureConnValid(t, conn)
	}
}

func TestInetCidrTranscodeIP(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	tests := []struct {
		sql   string
		value net.IP
	}{
		{"select $1::inet", net.ParseIP("0.0.0.0")},
		{"select $1::inet", net.ParseIP("127.0.0.1")},
		{"select $1::inet", net.ParseIP("12.34.56.0")},
		{"select $1::inet", net.ParseIP("255.255.255.255")},
		{"select $1::inet", net.ParseIP("::1")},
		{"select $1::inet", net.ParseIP("2607:f8b0:4009:80b::200e")},
		{"select $1::cidr", net.ParseIP("0.0.0.0")},
		{"select $1::cidr", net.ParseIP("127.0.0.1")},
		{"select $1::cidr", net.ParseIP("12.34.56.0")},
		{"select $1::cidr", net.ParseIP("255.255.255.255")},
		{"select $1::cidr", net.ParseIP("::1")},
		{"select $1::cidr", net.ParseIP("2607:f8b0:4009:80b::200e")},
	}

	for i, tt := range tests {
		var actual net.IP

		err := conn.QueryRow(tt.sql, tt.value).Scan(&actual)
		if err != nil {
			t.Errorf("%d. Unexpected failure: %v (sql -> %v, value -> %v)", i, err, tt.sql, tt.value)
			continue
		}

		if !actual.Equal(tt.value) {
			t.Errorf("%d. Expected %v, got %v (sql -> %v)", i, tt.value, actual, tt.sql)
		}

		ensureConnValid(t, conn)
	}

	failTests := []struct {
		sql   string
		value net.IPNet
	}{
		{"select $1::inet", mustParseCIDR(t, "192.168.1.0/24")},
		{"select $1::cidr", mustParseCIDR(t, "192.168.1.0/24")},
	}
	for i, tt := range failTests {
		var actual net.IP

		err := conn.QueryRow(tt.sql, tt.value).Scan(&actual)
		if !strings.Contains(err.Error(), "Cannot decode netmask") {
			t.Errorf("%d. Expected failure cannot decode netmask, but got: %v (sql -> %v, value -> %v)", i, err, tt.sql, tt.value)
			continue
		}

		ensureConnValid(t, conn)
	}
}

func TestInetCidrArrayTranscodeIPNet(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	tests := []struct {
		sql   string
		value []net.IPNet
	}{
		{
			"select $1::inet[]",
			[]net.IPNet{
				mustParseCIDR(t, "0.0.0.0/32"),
				mustParseCIDR(t, "127.0.0.1/32"),
				mustParseCIDR(t, "12.34.56.0/32"),
				mustParseCIDR(t, "192.168.1.0/24"),
				mustParseCIDR(t, "255.0.0.0/8"),
				mustParseCIDR(t, "255.255.255.255/32"),
				mustParseCIDR(t, "::/128"),
				mustParseCIDR(t, "::/0"),
				mustParseCIDR(t, "::1/128"),
				mustParseCIDR(t, "2607:f8b0:4009:80b::200e/128"),
			},
		},
		{
			"select $1::cidr[]",
			[]net.IPNet{
				mustParseCIDR(t, "0.0.0.0/32"),
				mustParseCIDR(t, "127.0.0.1/32"),
				mustParseCIDR(t, "12.34.56.0/32"),
				mustParseCIDR(t, "192.168.1.0/24"),
				mustParseCIDR(t, "255.0.0.0/8"),
				mustParseCIDR(t, "255.255.255.255/32"),
				mustParseCIDR(t, "::/128"),
				mustParseCIDR(t, "::/0"),
				mustParseCIDR(t, "::1/128"),
				mustParseCIDR(t, "2607:f8b0:4009:80b::200e/128"),
			},
		},
	}

	for i, tt := range tests {
		var actual []net.IPNet

		err := conn.QueryRow(tt.sql, tt.value).Scan(&actual)
		if err != nil {
			t.Errorf("%d. Unexpected failure: %v (sql -> %v, value -> %v)", i, err, tt.sql, tt.value)
			continue
		}

		if !reflect.DeepEqual(actual, tt.value) {
			t.Errorf("%d. Expected %v, got %v (sql -> %v)", i, tt.value, actual, tt.sql)
		}

		ensureConnValid(t, conn)
	}
}

func TestInetCidrArrayTranscodeIP(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	tests := []struct {
		sql   string
		value []net.IP
	}{
		{
			"select $1::inet[]",
			[]net.IP{
				net.ParseIP("0.0.0.0"),
				net.ParseIP("127.0.0.1"),
				net.ParseIP("12.34.56.0"),
				net.ParseIP("255.255.255.255"),
				net.ParseIP("2607:f8b0:4009:80b::200e"),
			},
		},
		{
			"select $1::cidr[]",
			[]net.IP{
				net.ParseIP("0.0.0.0"),
				net.ParseIP("127.0.0.1"),
				net.ParseIP("12.34.56.0"),
				net.ParseIP("255.255.255.255"),
				net.ParseIP("2607:f8b0:4009:80b::200e"),
			},
		},
	}

	for i, tt := range tests {
		var actual []net.IP

		err := conn.QueryRow(tt.sql, tt.value).Scan(&actual)
		if err != nil {
			t.Errorf("%d. Unexpected failure: %v (sql -> %v, value -> %v)", i, err, tt.sql, tt.value)
			continue
		}

		if !reflect.DeepEqual(actual, tt.value) {
			t.Errorf("%d. Expected %v, got %v (sql -> %v)", i, tt.value, actual, tt.sql)
		}

		ensureConnValid(t, conn)
	}

	failTests := []struct {
		sql   string
		value []net.IPNet
	}{
		{
			"select $1::inet[]",
			[]net.IPNet{
				mustParseCIDR(t, "12.34.56.0/32"),
				mustParseCIDR(t, "192.168.1.0/24"),
			},
		},
		{
			"select $1::cidr[]",
			[]net.IPNet{
				mustParseCIDR(t, "12.34.56.0/32"),
				mustParseCIDR(t, "192.168.1.0/24"),
			},
		},
	}

	for i, tt := range failTests {
		var actual []net.IP

		err := conn.QueryRow(tt.sql, tt.value).Scan(&actual)
		if err == nil || !strings.Contains(err.Error(), "Cannot decode netmask") {
			t.Errorf("%d. Expected failure cannot decode netmask, but got: %v (sql -> %v, value -> %v)", i, err, tt.sql, tt.value)
			continue
		}

		ensureConnValid(t, conn)
	}
}

func TestInetCidrTranscodeWithJustIP(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	tests := []struct {
		sql   string
		value string
	}{
		{"select $1::inet", "0.0.0.0/32"},
		{"select $1::inet", "127.0.0.1/32"},
		{"select $1::inet", "12.34.56.0/32"},
		{"select $1::inet", "255.255.255.255/32"},
		{"select $1::inet", "::/128"},
		{"select $1::inet", "2607:f8b0:4009:80b::200e/128"},
		{"select $1::cidr", "0.0.0.0/32"},
		{"select $1::cidr", "127.0.0.1/32"},
		{"select $1::cidr", "12.34.56.0/32"},
		{"select $1::cidr", "255.255.255.255/32"},
		{"select $1::cidr", "::/128"},
		{"select $1::cidr", "2607:f8b0:4009:80b::200e/128"},
	}

	for i, tt := range tests {
		expected := mustParseCIDR(t, tt.value)
		var actual net.IPNet

		err := conn.QueryRow(tt.sql, expected.IP).Scan(&actual)
		if err != nil {
			t.Errorf("%d. Unexpected failure: %v (sql -> %v, value -> %v)", i, err, tt.sql, tt.value)
			continue
		}

		if actual.String() != expected.String() {
			t.Errorf("%d. Expected %v, got %v (sql -> %v)", i, tt.value, actual, tt.sql)
		}

		ensureConnValid(t, conn)
	}
}

func TestNullX(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	type allTypes struct {
		s   pgx.NullString
		i16 pgx.NullInt16
		i32 pgx.NullInt32
		c   pgx.NullChar
		a   pgx.NullAclItem
		n   pgx.NullName
		oid pgx.NullOid
		xid pgx.NullXid
		cid pgx.NullCid
		tid pgx.NullTid
		i64 pgx.NullInt64
		f32 pgx.NullFloat32
		f64 pgx.NullFloat64
		b   pgx.NullBool
		t   pgx.NullTime
	}

	var actual, zero allTypes

	tests := []struct {
		sql       string
		queryArgs []interface{}
		scanArgs  []interface{}
		expected  allTypes
	}{
		{"select $1::text", []interface{}{pgx.NullString{String: "foo", Valid: true}}, []interface{}{&actual.s}, allTypes{s: pgx.NullString{String: "foo", Valid: true}}},
		{"select $1::text", []interface{}{pgx.NullString{String: "foo", Valid: false}}, []interface{}{&actual.s}, allTypes{s: pgx.NullString{String: "", Valid: false}}},
		{"select $1::int2", []interface{}{pgx.NullInt16{Int16: 1, Valid: true}}, []interface{}{&actual.i16}, allTypes{i16: pgx.NullInt16{Int16: 1, Valid: true}}},
		{"select $1::int2", []interface{}{pgx.NullInt16{Int16: 1, Valid: false}}, []interface{}{&actual.i16}, allTypes{i16: pgx.NullInt16{Int16: 0, Valid: false}}},
		{"select $1::int4", []interface{}{pgx.NullInt32{Int32: 1, Valid: true}}, []interface{}{&actual.i32}, allTypes{i32: pgx.NullInt32{Int32: 1, Valid: true}}},
		{"select $1::int4", []interface{}{pgx.NullInt32{Int32: 1, Valid: false}}, []interface{}{&actual.i32}, allTypes{i32: pgx.NullInt32{Int32: 0, Valid: false}}},
		{"select $1::oid", []interface{}{pgx.NullOid{Oid: 1, Valid: true}}, []interface{}{&actual.oid}, allTypes{oid: pgx.NullOid{Oid: 1, Valid: true}}},
		{"select $1::oid", []interface{}{pgx.NullOid{Oid: 1, Valid: false}}, []interface{}{&actual.oid}, allTypes{oid: pgx.NullOid{Oid: 0, Valid: false}}},
		{"select $1::oid", []interface{}{pgx.NullOid{Oid: 4294967295, Valid: true}}, []interface{}{&actual.oid}, allTypes{oid: pgx.NullOid{Oid: 4294967295, Valid: true}}},
		{"select $1::xid", []interface{}{pgx.NullXid{Xid: 1, Valid: true}}, []interface{}{&actual.xid}, allTypes{xid: pgx.NullXid{Xid: 1, Valid: true}}},
		{"select $1::xid", []interface{}{pgx.NullXid{Xid: 1, Valid: false}}, []interface{}{&actual.xid}, allTypes{xid: pgx.NullXid{Xid: 0, Valid: false}}},
		{"select $1::xid", []interface{}{pgx.NullXid{Xid: 4294967295, Valid: true}}, []interface{}{&actual.xid}, allTypes{xid: pgx.NullXid{Xid: 4294967295, Valid: true}}},
		{"select $1::\"char\"", []interface{}{pgx.NullChar{Char: 1, Valid: true}}, []interface{}{&actual.c}, allTypes{c: pgx.NullChar{Char: 1, Valid: true}}},
		{"select $1::\"char\"", []interface{}{pgx.NullChar{Char: 1, Valid: false}}, []interface{}{&actual.c}, allTypes{c: pgx.NullChar{Char: 0, Valid: false}}},
		{"select $1::\"char\"", []interface{}{pgx.NullChar{Char: 255, Valid: true}}, []interface{}{&actual.c}, allTypes{c: pgx.NullChar{Char: 255, Valid: true}}},
		{"select $1::name", []interface{}{pgx.NullName{Name: "foo", Valid: true}}, []interface{}{&actual.n}, allTypes{n: pgx.NullName{Name: "foo", Valid: true}}},
		{"select $1::name", []interface{}{pgx.NullName{Name: "foo", Valid: false}}, []interface{}{&actual.n}, allTypes{n: pgx.NullName{Name: "", Valid: false}}},
		{"select $1::aclitem", []interface{}{pgx.NullAclItem{AclItem: "postgres=arwdDxt/postgres", Valid: true}}, []interface{}{&actual.a}, allTypes{a: pgx.NullAclItem{AclItem: "postgres=arwdDxt/postgres", Valid: true}}},
		{"select $1::aclitem", []interface{}{pgx.NullAclItem{AclItem: "postgres=arwdDxt/postgres", Valid: false}}, []interface{}{&actual.a}, allTypes{a: pgx.NullAclItem{AclItem: "", Valid: false}}},
		// A tricky (and valid) aclitem can still be used, especially with Go's useful backticks
		{"select $1::aclitem", []interface{}{pgx.NullAclItem{AclItem: `postgres=arwdDxt/" tricky, ' } "" \ test user "`, Valid: true}}, []interface{}{&actual.a}, allTypes{a: pgx.NullAclItem{AclItem: `postgres=arwdDxt/" tricky, ' } "" \ test user "`, Valid: true}}},
		{"select $1::cid", []interface{}{pgx.NullCid{Cid: 1, Valid: true}}, []interface{}{&actual.cid}, allTypes{cid: pgx.NullCid{Cid: 1, Valid: true}}},
		{"select $1::cid", []interface{}{pgx.NullCid{Cid: 1, Valid: false}}, []interface{}{&actual.cid}, allTypes{cid: pgx.NullCid{Cid: 0, Valid: false}}},
		{"select $1::cid", []interface{}{pgx.NullCid{Cid: 4294967295, Valid: true}}, []interface{}{&actual.cid}, allTypes{cid: pgx.NullCid{Cid: 4294967295, Valid: true}}},
		{"select $1::tid", []interface{}{pgx.NullTid{Tid: pgx.Tid{BlockNumber: 1, OffsetNumber: 1}, Valid: true}}, []interface{}{&actual.tid}, allTypes{tid: pgx.NullTid{Tid: pgx.Tid{BlockNumber: 1, OffsetNumber: 1}, Valid: true}}},
		{"select $1::tid", []interface{}{pgx.NullTid{Tid: pgx.Tid{BlockNumber: 1, OffsetNumber: 1}, Valid: false}}, []interface{}{&actual.tid}, allTypes{tid: pgx.NullTid{Tid: pgx.Tid{BlockNumber: 0, OffsetNumber: 0}, Valid: false}}},
		{"select $1::tid", []interface{}{pgx.NullTid{Tid: pgx.Tid{BlockNumber: 4294967295, OffsetNumber: 65535}, Valid: true}}, []interface{}{&actual.tid}, allTypes{tid: pgx.NullTid{Tid: pgx.Tid{BlockNumber: 4294967295, OffsetNumber: 65535}, Valid: true}}},
		{"select $1::int8", []interface{}{pgx.NullInt64{Int64: 1, Valid: true}}, []interface{}{&actual.i64}, allTypes{i64: pgx.NullInt64{Int64: 1, Valid: true}}},
		{"select $1::int8", []interface{}{pgx.NullInt64{Int64: 1, Valid: false}}, []interface{}{&actual.i64}, allTypes{i64: pgx.NullInt64{Int64: 0, Valid: false}}},
		{"select $1::float4", []interface{}{pgx.NullFloat32{Float32: 1.23, Valid: true}}, []interface{}{&actual.f32}, allTypes{f32: pgx.NullFloat32{Float32: 1.23, Valid: true}}},
		{"select $1::float4", []interface{}{pgx.NullFloat32{Float32: 1.23, Valid: false}}, []interface{}{&actual.f32}, allTypes{f32: pgx.NullFloat32{Float32: 0, Valid: false}}},
		{"select $1::float8", []interface{}{pgx.NullFloat64{Float64: 1.23, Valid: true}}, []interface{}{&actual.f64}, allTypes{f64: pgx.NullFloat64{Float64: 1.23, Valid: true}}},
		{"select $1::float8", []interface{}{pgx.NullFloat64{Float64: 1.23, Valid: false}}, []interface{}{&actual.f64}, allTypes{f64: pgx.NullFloat64{Float64: 0, Valid: false}}},
		{"select $1::bool", []interface{}{pgx.NullBool{Bool: true, Valid: true}}, []interface{}{&actual.b}, allTypes{b: pgx.NullBool{Bool: true, Valid: true}}},
		{"select $1::bool", []interface{}{pgx.NullBool{Bool: true, Valid: false}}, []interface{}{&actual.b}, allTypes{b: pgx.NullBool{Bool: false, Valid: false}}},
		{"select $1::timestamptz", []interface{}{pgx.NullTime{Time: time.Unix(123, 5000), Valid: true}}, []interface{}{&actual.t}, allTypes{t: pgx.NullTime{Time: time.Unix(123, 5000), Valid: true}}},
		{"select $1::timestamptz", []interface{}{pgx.NullTime{Time: time.Unix(123, 5000), Valid: false}}, []interface{}{&actual.t}, allTypes{t: pgx.NullTime{Time: time.Time{}, Valid: false}}},
		{"select $1::timestamp", []interface{}{pgx.NullTime{Time: time.Unix(123, 5000), Valid: true}}, []interface{}{&actual.t}, allTypes{t: pgx.NullTime{Time: time.Unix(123, 5000), Valid: true}}},
		{"select $1::timestamp", []interface{}{pgx.NullTime{Time: time.Unix(123, 5000), Valid: false}}, []interface{}{&actual.t}, allTypes{t: pgx.NullTime{Time: time.Time{}, Valid: false}}},
		{"select $1::date", []interface{}{pgx.NullTime{Time: time.Date(1990, 1, 1, 0, 0, 0, 0, time.Local), Valid: true}}, []interface{}{&actual.t}, allTypes{t: pgx.NullTime{Time: time.Date(1990, 1, 1, 0, 0, 0, 0, time.Local), Valid: true}}},
		{"select $1::date", []interface{}{pgx.NullTime{Time: time.Date(1990, 1, 1, 0, 0, 0, 0, time.Local), Valid: false}}, []interface{}{&actual.t}, allTypes{t: pgx.NullTime{Time: time.Time{}, Valid: false}}},
		{"select 42::int4, $1::float8", []interface{}{pgx.NullFloat64{Float64: 1.23, Valid: true}}, []interface{}{&actual.i32, &actual.f64}, allTypes{i32: pgx.NullInt32{Int32: 42, Valid: true}, f64: pgx.NullFloat64{Float64: 1.23, Valid: true}}},
	}

	for i, tt := range tests {
		actual = zero

		err := conn.QueryRow(tt.sql, tt.queryArgs...).Scan(tt.scanArgs...)
		if err != nil {
			t.Errorf("%d. Unexpected failure: %v (sql -> %v, queryArgs -> %v)", i, err, tt.sql, tt.queryArgs)
		}

		if actual != tt.expected {
			t.Errorf("%d. Expected %v, got %v (sql -> %v, queryArgs -> %v)", i, tt.expected, actual, tt.sql, tt.queryArgs)
		}

		ensureConnValid(t, conn)
	}
}

func assertAclItemSlicesEqual(t *testing.T, query, scan []pgx.AclItem) {
	if !reflect.DeepEqual(query, scan) {
		t.Errorf("failed to encode aclitem[]\n EXPECTED: %d %v\n ACTUAL:   %d %v", len(query), query, len(scan), scan)
	}
}

func TestAclArrayDecoding(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	sql := "select $1::aclitem[]"
	var scan []pgx.AclItem

	tests := []struct {
		query []pgx.AclItem
	}{
		{
			[]pgx.AclItem{},
		},
		{
			[]pgx.AclItem{"=r/postgres"},
		},
		{
			[]pgx.AclItem{"=r/postgres", "postgres=arwdDxt/postgres"},
		},
		{
			[]pgx.AclItem{"=r/postgres", "postgres=arwdDxt/postgres", `postgres=arwdDxt/" tricky, ' } "" \ test user "`},
		},
	}
	for i, tt := range tests {
		err := conn.QueryRow(sql, tt.query).Scan(&scan)
		if err != nil {
			// t.Errorf(`%d. error reading array: %v`, i, err)
			t.Errorf(`%d. error reading array: %v query: %s`, i, err, tt.query)
			if pgerr, ok := err.(pgx.PgError); ok {
				t.Errorf(`%d. error reading array (detail): %s`, i, pgerr.Detail)
			}
			continue
		}
		assertAclItemSlicesEqual(t, tt.query, scan)
		ensureConnValid(t, conn)
	}
}

func TestArrayDecoding(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	tests := []struct {
		sql    string
		query  interface{}
		scan   interface{}
		assert func(*testing.T, interface{}, interface{})
	}{
		{
			"select $1::bool[]", []bool{true, false, true}, &[]bool{},
			func(t *testing.T, query, scan interface{}) {
				if !reflect.DeepEqual(query, *(scan.(*[]bool))) {
					t.Errorf("failed to encode bool[]")
				}
			},
		},
		{
			"select $1::smallint[]", []int16{2, 4, 484, 32767}, &[]int16{},
			func(t *testing.T, query, scan interface{}) {
				if !reflect.DeepEqual(query, *(scan.(*[]int16))) {
					t.Errorf("failed to encode smallint[]")
				}
			},
		},
		{
			"select $1::smallint[]", []uint16{2, 4, 484, 32767}, &[]uint16{},
			func(t *testing.T, query, scan interface{}) {
				if !reflect.DeepEqual(query, *(scan.(*[]uint16))) {
					t.Errorf("failed to encode smallint[]")
				}
			},
		},
		{
			"select $1::int[]", []int32{2, 4, 484}, &[]int32{},
			func(t *testing.T, query, scan interface{}) {
				if !reflect.DeepEqual(query, *(scan.(*[]int32))) {
					t.Errorf("failed to encode int[]")
				}
			},
		},
		{
			"select $1::int[]", []uint32{2, 4, 484, 2147483647}, &[]uint32{},
			func(t *testing.T, query, scan interface{}) {
				if !reflect.DeepEqual(query, *(scan.(*[]uint32))) {
					t.Errorf("failed to encode int[]")
				}
			},
		},
		{
			"select $1::bigint[]", []int64{2, 4, 484, 9223372036854775807}, &[]int64{},
			func(t *testing.T, query, scan interface{}) {
				if !reflect.DeepEqual(query, *(scan.(*[]int64))) {
					t.Errorf("failed to encode bigint[]")
				}
			},
		},
		{
			"select $1::bigint[]", []uint64{2, 4, 484, 9223372036854775807}, &[]uint64{},
			func(t *testing.T, query, scan interface{}) {
				if !reflect.DeepEqual(query, *(scan.(*[]uint64))) {
					t.Errorf("failed to encode bigint[]")
				}
			},
		},
		{
			"select $1::text[]", []string{"it's", "over", "9000!"}, &[]string{},
			func(t *testing.T, query, scan interface{}) {
				if !reflect.DeepEqual(query, *(scan.(*[]string))) {
					t.Errorf("failed to encode text[]")
				}
			},
		},
		{
			"select $1::uuid[]", pgx.UUIDs{"01086ee0-4963-4e35-9116-30c173a8d0bd", "01086ee0-4963-4e35-9116-aaaaaaaaaaaa"}, &pgx.UUIDs{},
			func(t *testing.T, query, scan interface{}) {
				if !reflect.DeepEqual(query, *(scan.(*pgx.UUIDs))) {
					t.Fatalf("failed to encode uuid[]")
				}
			},
		},
		{
			"select $1::timestamp[]", []time.Time{time.Unix(323232, 0), time.Unix(3239949334, 00)}, &[]time.Time{},
			func(t *testing.T, query, scan interface{}) {
				if !reflect.DeepEqual(query, *(scan.(*[]time.Time))) {
					t.Errorf("failed to encode time.Time[] to timestamp[]")
				}
			},
		},
		{
			"select $1::timestamptz[]", []time.Time{time.Unix(323232, 0), time.Unix(3239949334, 00)}, &[]time.Time{},
			func(t *testing.T, query, scan interface{}) {
				if !reflect.DeepEqual(query, *(scan.(*[]time.Time))) {
					t.Errorf("failed to encode time.Time[] to timestamptz[]")
				}
			},
		},
		{
			"select $1::bytea[]", [][]byte{{0, 1, 2, 3}, {4, 5, 6, 7}}, &[][]byte{},
			func(t *testing.T, query, scan interface{}) {
				queryBytesSliceSlice := query.([][]byte)
				scanBytesSliceSlice := *(scan.(*[][]byte))
				if len(queryBytesSliceSlice) != len(scanBytesSliceSlice) {
					t.Errorf("failed to encode byte[][] to bytea[]: expected %d to equal %d", len(queryBytesSliceSlice), len(scanBytesSliceSlice))
				}
				for i := range queryBytesSliceSlice {
					qb := queryBytesSliceSlice[i]
					sb := scanBytesSliceSlice[i]
					if !bytes.Equal(qb, sb) {
						t.Errorf("failed to encode byte[][] to bytea[]: expected %v to equal %v", qb, sb)
					}
				}
			},
		},
	}

	for i, tt := range tests {
		err := conn.QueryRow(tt.sql, tt.query).Scan(tt.scan)
		if err != nil {
			t.Errorf(`%d. error reading array: %v`, i, err)
			continue
		}
		tt.assert(t, tt.query, tt.scan)
		ensureConnValid(t, conn)
	}
}

type shortScanner struct{}

func (*shortScanner) Scan(r *pgx.ValueReader) error {
	r.ReadByte()
	return nil
}

func TestShortScanner(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	rows, err := conn.Query("select 'ab', 'cd' union select 'cd', 'ef'")
	if err != nil {
		t.Error(err)
	}
	defer rows.Close()

	for rows.Next() {
		var s1, s2 shortScanner
		err = rows.Scan(&s1, &s2)
		if err != nil {
			t.Error(err)
		}
	}

	ensureConnValid(t, conn)
}

func TestEmptyArrayDecoding(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	var val []string

	err := conn.QueryRow("select array[]::text[]").Scan(&val)
	if err != nil {
		t.Errorf(`error reading array: %v`, err)
	}
	if len(val) != 0 {
		t.Errorf("Expected 0 values, got %d", len(val))
	}

	var n, m int32

	err = conn.QueryRow("select 1::integer, array[]::text[], 42::integer").Scan(&n, &val, &m)
	if err != nil {
		t.Errorf(`error reading array: %v`, err)
	}
	if len(val) != 0 {
		t.Errorf("Expected 0 values, got %d", len(val))
	}
	if n != 1 {
		t.Errorf("Expected n to be 1, but it was %d", n)
	}
	if m != 42 {
		t.Errorf("Expected n to be 42, but it was %d", n)
	}

	rows, err := conn.Query("select 1::integer, array['test']::text[] union select 2::integer, array[]::text[] union select 3::integer, array['test']::text[]")
	if err != nil {
		t.Errorf(`error retrieving rows with array: %v`, err)
	}
	defer rows.Close()

	for rows.Next() {
		err = rows.Scan(&n, &val)
		if err != nil {
			t.Errorf(`error reading array: %v`, err)
		}
	}

	ensureConnValid(t, conn)
}

func TestNullXMismatch(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	type allTypes struct {
		s   pgx.NullString
		i16 pgx.NullInt16
		i32 pgx.NullInt32
		i64 pgx.NullInt64
		f32 pgx.NullFloat32
		f64 pgx.NullFloat64
		b   pgx.NullBool
		t   pgx.NullTime
	}

	var actual, zero allTypes

	tests := []struct {
		sql       string
		queryArgs []interface{}
		scanArgs  []interface{}
		err       string
	}{
		{"select $1::date", []interface{}{pgx.NullString{String: "foo", Valid: true}}, []interface{}{&actual.s}, "invalid input syntax for type date"},
		{"select $1::date", []interface{}{pgx.NullInt16{Int16: 1, Valid: true}}, []interface{}{&actual.i16}, "cannot encode into OID 1082"},
		{"select $1::date", []interface{}{pgx.NullInt32{Int32: 1, Valid: true}}, []interface{}{&actual.i32}, "cannot encode into OID 1082"},
		{"select $1::date", []interface{}{pgx.NullInt64{Int64: 1, Valid: true}}, []interface{}{&actual.i64}, "cannot encode into OID 1082"},
		{"select $1::date", []interface{}{pgx.NullFloat32{Float32: 1.23, Valid: true}}, []interface{}{&actual.f32}, "cannot encode into OID 1082"},
		{"select $1::date", []interface{}{pgx.NullFloat64{Float64: 1.23, Valid: true}}, []interface{}{&actual.f64}, "cannot encode into OID 1082"},
		{"select $1::date", []interface{}{pgx.NullBool{Bool: true, Valid: true}}, []interface{}{&actual.b}, "cannot encode into OID 1082"},
		{"select $1::int4", []interface{}{pgx.NullTime{Time: time.Unix(123, 5000), Valid: true}}, []interface{}{&actual.t}, "cannot encode into OID 23"},
	}

	for i, tt := range tests {
		actual = zero

		err := conn.QueryRow(tt.sql, tt.queryArgs...).Scan(tt.scanArgs...)
		if err == nil || !strings.Contains(err.Error(), tt.err) {
			t.Errorf(`%d. Expected error to contain "%s", but it didn't: %v`, i, tt.err, err)
		}

		ensureConnValid(t, conn)
	}
}

func TestPointerPointer(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	type allTypes struct {
		s   *string
		i16 *int16
		i32 *int32
		i64 *int64
		f32 *float32
		f64 *float64
		b   *bool
		t   *time.Time
	}

	var actual, zero, expected allTypes

	{
		s := "foo"
		expected.s = &s
		i16 := int16(1)
		expected.i16 = &i16
		i32 := int32(1)
		expected.i32 = &i32
		i64 := int64(1)
		expected.i64 = &i64
		f32 := float32(1.23)
		expected.f32 = &f32
		f64 := float64(1.23)
		expected.f64 = &f64
		b := true
		expected.b = &b
		t := time.Unix(123, 5000)
		expected.t = &t
	}

	tests := []struct {
		sql       string
		queryArgs []interface{}
		scanArgs  []interface{}
		expected  allTypes
	}{
		{"select $1::text", []interface{}{expected.s}, []interface{}{&actual.s}, allTypes{s: expected.s}},
		{"select $1::text", []interface{}{zero.s}, []interface{}{&actual.s}, allTypes{}},
		{"select $1::int2", []interface{}{expected.i16}, []interface{}{&actual.i16}, allTypes{i16: expected.i16}},
		{"select $1::int2", []interface{}{zero.i16}, []interface{}{&actual.i16}, allTypes{}},
		{"select $1::int4", []interface{}{expected.i32}, []interface{}{&actual.i32}, allTypes{i32: expected.i32}},
		{"select $1::int4", []interface{}{zero.i32}, []interface{}{&actual.i32}, allTypes{}},
		{"select $1::int8", []interface{}{expected.i64}, []interface{}{&actual.i64}, allTypes{i64: expected.i64}},
		{"select $1::int8", []interface{}{zero.i64}, []interface{}{&actual.i64}, allTypes{}},
		{"select $1::float4", []interface{}{expected.f32}, []interface{}{&actual.f32}, allTypes{f32: expected.f32}},
		{"select $1::float4", []interface{}{zero.f32}, []interface{}{&actual.f32}, allTypes{}},
		{"select $1::float8", []interface{}{expected.f64}, []interface{}{&actual.f64}, allTypes{f64: expected.f64}},
		{"select $1::float8", []interface{}{zero.f64}, []interface{}{&actual.f64}, allTypes{}},
		{"select $1::bool", []interface{}{expected.b}, []interface{}{&actual.b}, allTypes{b: expected.b}},
		{"select $1::bool", []interface{}{zero.b}, []interface{}{&actual.b}, allTypes{}},
		{"select $1::timestamptz", []interface{}{expected.t}, []interface{}{&actual.t}, allTypes{t: expected.t}},
		{"select $1::timestamptz", []interface{}{zero.t}, []interface{}{&actual.t}, allTypes{}},
		{"select $1::timestamp", []interface{}{expected.t}, []interface{}{&actual.t}, allTypes{t: expected.t}},
		{"select $1::timestamp", []interface{}{zero.t}, []interface{}{&actual.t}, allTypes{}},
	}

	for i, tt := range tests {
		actual = zero

		err := conn.QueryRow(tt.sql, tt.queryArgs...).Scan(tt.scanArgs...)
		if err != nil {
			t.Errorf("%d. Unexpected failure: %v (sql -> %v, queryArgs -> %v)", i, err, tt.sql, tt.queryArgs)
		}

		if !reflect.DeepEqual(actual, tt.expected) {
			t.Errorf("%d. Expected %v, got %v (sql -> %v, queryArgs -> %v)", i, tt.expected, actual, tt.sql, tt.queryArgs)
		}

		ensureConnValid(t, conn)
	}
}

func TestPointerPointerNonZero(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	f := "foo"
	dest := &f

	err := conn.QueryRow("select $1::text", nil).Scan(&dest)
	if err != nil {
		t.Errorf("Unexpected failure scanning: %v", err)
	}
	if dest != nil {
		t.Errorf("Expected dest to be nil, got %#v", dest)
	}
}

func TestEncodeTypeRename(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	type _int int
	inInt := _int(3)
	var outInt _int

	type _int8 int8
	inInt8 := _int8(3)
	var outInt8 _int8

	type _int16 int16
	inInt16 := _int16(3)
	var outInt16 _int16

	type _int32 int32
	inInt32 := _int32(4)
	var outInt32 _int32

	type _int64 int64
	inInt64 := _int64(5)
	var outInt64 _int64

	type _uint uint
	inUint := _uint(6)
	var outUint _uint

	type _uint8 uint8
	inUint8 := _uint8(7)
	var outUint8 _uint8

	type _uint16 uint16
	inUint16 := _uint16(8)
	var outUint16 _uint16

	type _uint32 uint32
	inUint32 := _uint32(9)
	var outUint32 _uint32

	type _uint64 uint64
	inUint64 := _uint64(10)
	var outUint64 _uint64

	type _string string
	inString := _string("foo")
	var outString _string

	err := conn.QueryRow("select $1::int, $2::int, $3::int2, $4::int4, $5::int8, $6::int, $7::int, $8::int, $9::int, $10::int, $11::text",
		inInt, inInt8, inInt16, inInt32, inInt64, inUint, inUint8, inUint16, inUint32, inUint64, inString,
	).Scan(&outInt, &outInt8, &outInt16, &outInt32, &outInt64, &outUint, &outUint8, &outUint16, &outUint32, &outUint64, &outString)
	if err != nil {
		t.Fatalf("Failed with type rename: %v", err)
	}

	if inInt != outInt {
		t.Errorf("int rename: expected %v, got %v", inInt, outInt)
	}

	if inInt8 != outInt8 {
		t.Errorf("int8 rename: expected %v, got %v", inInt8, outInt8)
	}

	if inInt16 != outInt16 {
		t.Errorf("int16 rename: expected %v, got %v", inInt16, outInt16)
	}

	if inInt32 != outInt32 {
		t.Errorf("int32 rename: expected %v, got %v", inInt32, outInt32)
	}

	if inInt64 != outInt64 {
		t.Errorf("int64 rename: expected %v, got %v", inInt64, outInt64)
	}

	if inUint != outUint {
		t.Errorf("uint rename: expected %v, got %v", inUint, outUint)
	}

	if inUint8 != outUint8 {
		t.Errorf("uint8 rename: expected %v, got %v", inUint8, outUint8)
	}

	if inUint16 != outUint16 {
		t.Errorf("uint16 rename: expected %v, got %v", inUint16, outUint16)
	}

	if inUint32 != outUint32 {
		t.Errorf("uint32 rename: expected %v, got %v", inUint32, outUint32)
	}

	if inUint64 != outUint64 {
		t.Errorf("uint64 rename: expected %v, got %v", inUint64, outUint64)
	}

	if inString != outString {
		t.Errorf("string rename: expected %v, got %v", inString, outString)
	}

	ensureConnValid(t, conn)
}

func TestRowDecode(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	tests := []struct {
		sql      string
		expected []interface{}
	}{
		{
			"select row(1, 'cat', '2015-01-01 08:12:42-00'::timestamptz)",
			[]interface{}{
				int32(1),
				"cat",
				time.Date(2015, 1, 1, 8, 12, 42, 0, time.UTC).Local(),
			},
		},
	}

	for i, tt := range tests {
		var actual []interface{}

		err := conn.QueryRow(tt.sql).Scan(&actual)
		if err != nil {
			t.Errorf("%d. Unexpected failure: %v (sql -> %v)", i, err, tt.sql)
			continue
		}

		if !reflect.DeepEqual(actual, tt.expected) {
			t.Errorf("%d. Expected %v, got %v (sql -> %v)", i, tt.expected, actual, tt.sql)
		}

		ensureConnValid(t, conn)
	}
}

func TestUUID(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	f := "01086ee0-4963-4e35-9116-30c173a8d0bd"

	{
		dest := ""
		if err := conn.QueryRow("select $1::uuid", f).Scan(&dest); err != nil {
			t.Errorf("Unexpected failure scanning: %v", err)
		}
		if have, want := dest, f; have != want {
			t.Errorf("have %q, want %q", have, want)
		}
	}

	{
		dest := ""
		if err := conn.QueryRow("select $1::uuid", &f).Scan(&dest); err != nil {
			t.Errorf("Unexpected failure scanning: %v", err)
		}
		if have, want := dest, f; have != want {
			t.Errorf("have %q, want %q", have, want)
		}
	}

	{
		dest := pgx.NullString{}
		if err := conn.QueryRow("select $1::uuid", &f).Scan(&dest); err != nil {
			t.Errorf("Unexpected failure scanning: %v", err)
		}
		if have, want := dest, (pgx.NullString{String: f, Valid: true}); have != want {
			t.Errorf("have %+v, want %+v", have, want)
		}
	}
}
