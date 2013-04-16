package pgx

import (
	"strings"
	"testing"
)

var sharedConn *conn

func getSharedConn() (c *conn) {
	if sharedConn == nil {
		var err error
		sharedConn, err = Connect(map[string]string{"socket": "/private/tmp/.s.PGSQL.5432", "user": "pgx_none", "database": "pgx_test"})
		if err != nil {
			panic("Unable to establish connection")
		}

	}
	return sharedConn
}

func TestConnect(t *testing.T) {
	conn, err := Connect(map[string]string{"socket": "/private/tmp/.s.PGSQL.5432", "user": "pgx_none", "database": "pgx_test"})
	if err != nil {
		t.Fatal("Unable to establish connection")
	}

	if _, present := conn.runtimeParams["server_version"]; !present {
		t.Error("Runtime parameters not stored")
	}

	if conn.pid == 0 {
		t.Error("Backend PID not stored")
	}

	if conn.secretKey == 0 {
		t.Error("Backend secret key not stored")
	}

	var rows []map[string]string
	rows, err = conn.Query("select current_database()")
	if err != nil || rows[0]["current_database"] != "pgx_test" {
		t.Error("Did not connect to specified database (pgx_text)")
	}

	rows, err = conn.Query("select current_user")
	if err != nil || rows[0]["current_user"] != "pgx_none" {
		t.Error("Did not connect as specified user (pgx_none)")
	}

	err = conn.Close()
	if err != nil {
		t.Fatal("Unable to close connection")
	}
}

func TestConnectWithInvalidUser(t *testing.T) {
	_, err := Connect(map[string]string{"socket": "/private/tmp/.s.PGSQL.5432", "user": "invalid_user", "database": "pgx_test"})
	pgErr := err.(PgError)
	if pgErr.Code != "28000" {
		t.Fatal("Did not receive expected error when connecting with invalid user")
	}
}

func TestConnectWithPlainTextPassword(t *testing.T) {
	conn, err := Connect(map[string]string{"socket": "/private/tmp/.s.PGSQL.5432", "user": "pgx_pw", "password": "secret", "database": "pgx_test"})
	if err != nil {
		t.Fatal("Unable to establish connection: " + err.Error())
	}

	err = conn.Close()
	if err != nil {
		t.Fatal("Unable to close connection")
	}
}

func TestConnectWithMD5Password(t *testing.T) {
	conn, err := Connect(map[string]string{"socket": "/private/tmp/.s.PGSQL.5432", "user": "pgx_md5", "password": "secret", "database": "pgx_test"})
	if err != nil {
		t.Fatal("Unable to establish connection: " + err.Error())
	}

	err = conn.Close()
	if err != nil {
		t.Fatal("Unable to close connection")
	}
}

func TestQuery(t *testing.T) {
	conn := getSharedConn()

	rows, err := conn.Query("select $1 as name", "Jack")
	if err != nil {
		t.Fatal("Query failed")
	}

	if len(rows) != 1 {
		t.Fatal("Received wrong number of rows")
	}

	if rows[0]["name"] != "Jack" {
		t.Fatal("Received incorrect name")
	}
}

func TestSelectString(t *testing.T) {
	conn := getSharedConn()

	s, err := conn.SelectString("select $1", "foo")
	if err != nil {
		t.Fatal("Unable to select string: " + err.Error())
	}

	if s != "foo" {
		t.Error("Received incorrect string")
	}
}

func TestSelectInt64(t *testing.T) {
	conn := getSharedConn()

	i, err := conn.SelectInt64("select $1", 1)
	if err != nil {
		t.Fatal("Unable to select int64: " + err.Error())
	}

	if i != 1 {
		t.Error("Received incorrect int64")
	}

	i, err = conn.SelectInt64("select power(2,65)::numeric")
	if err == nil || !strings.Contains(err.Error(), "value out of range") {
		t.Error("Expected value out of range error when selecting number greater than max int64")
	}

	i, err = conn.SelectInt64("select -power(2,65)::numeric")
	if err == nil || !strings.Contains(err.Error(), "value out of range") {
		t.Error("Expected value out of range error when selecting number less than min int64")
	}
}

func TestSelectInt32(t *testing.T) {
	conn := getSharedConn()

	i, err := conn.SelectInt32("select $1", 1)
	if err != nil {
		t.Fatal("Unable to select int32: " + err.Error())
	}

	if i != 1 {
		t.Error("Received incorrect int32")
	}

	i, err = conn.SelectInt32("select power(2,33)::numeric")
	if err == nil || !strings.Contains(err.Error(), "value out of range") {
		t.Error("Expected value out of range error when selecting number greater than max int32")
	}

	i, err = conn.SelectInt32("select -power(2,33)::numeric")
	if err == nil || !strings.Contains(err.Error(), "value out of range") {
		t.Error("Expected value out of range error when selecting number less than min int32")
	}
}

func TestSelectInt16(t *testing.T) {
	conn := getSharedConn()

	i, err := conn.SelectInt16("select $1", 1)
	if err != nil {
		t.Fatal("Unable to select int16: " + err.Error())
	}

	if i != 1 {
		t.Error("Received incorrect int16")
	}

	i, err = conn.SelectInt16("select power(2,17)::numeric")
	if err == nil || !strings.Contains(err.Error(), "value out of range") {
		t.Error("Expected value out of range error when selecting number greater than max int16")
	}

	i, err = conn.SelectInt16("select -power(2,17)::numeric")
	if err == nil || !strings.Contains(err.Error(), "value out of range") {
		t.Error("Expected value out of range error when selecting number less than min int16")
	}
}

func TestSelectFloat64(t *testing.T) {
	conn := getSharedConn()

	f, err := conn.SelectFloat64("select $1", 1.23)
	if err != nil {
		t.Fatal("Unable to select float64: " + err.Error())
	}

	if f != 1.23 {
		t.Error("Received incorrect float64")
	}
}

func TestSelectFloat32(t *testing.T) {
	conn := getSharedConn()

	f, err := conn.SelectFloat32("select $1", 1.23)
	if err != nil {
		t.Fatal("Unable to select float32: " + err.Error())
	}

	if f != 1.23 {
		t.Error("Received incorrect float32")
	}
}

func TestSelectAllString(t *testing.T) {
	conn := getSharedConn()

	s, err := conn.SelectAllString("select * from (values ($1), ($2), ($3), ($4)) t", "Matthew", "Mark", "Luke", "John")
	if err != nil {
		t.Fatal("Unable to select all strings: " + err.Error())
	}

	if s[0] != "Matthew" || s[1] != "Mark" || s[2] != "Luke" || s[3] != "John" {
		t.Error("Received incorrect strings")
	}
}

func TestSelectAllInt64(t *testing.T) {
	conn := getSharedConn()

	i, err := conn.SelectAllInt64("select * from (values ($1), ($2)) t", 1, 2)
	if err != nil {
		t.Fatal("Unable to select all int64: " + err.Error())
	}

	if i[0] != 1 || i[1] != 2 {
		t.Error("Received incorrect int64s")
	}

	i, err = conn.SelectAllInt64("select power(2,65)::numeric")
	if err == nil || !strings.Contains(err.Error(), "value out of range") {
		t.Error("Expected value out of range error when selecting number greater than max int64")
	}

	i, err = conn.SelectAllInt64("select -power(2,65)::numeric")
	if err == nil || !strings.Contains(err.Error(), "value out of range") {
		t.Error("Expected value out of range error when selecting number less than min int64")
	}
}

func TestSelectAllInt32(t *testing.T) {
	conn := getSharedConn()

	i, err := conn.SelectAllInt32("select * from (values ($1), ($2)) t", 1, 2)
	if err != nil {
		t.Fatal("Unable to select all int32: " + err.Error())
	}

	if i[0] != 1 || i[1] != 2 {
		t.Error("Received incorrect int32")
	}

	i, err = conn.SelectAllInt32("select power(2,33)::numeric")
	if err == nil || !strings.Contains(err.Error(), "value out of range") {
		t.Error("Expected value out of range error when selecting number greater than max int32")
	}

	i, err = conn.SelectAllInt32("select -power(2,33)::numeric")
	if err == nil || !strings.Contains(err.Error(), "value out of range") {
		t.Error("Expected value out of range error when selecting number less than min int32")
	}
}

func TestSelectAllInt16(t *testing.T) {
	conn := getSharedConn()

	i, err := conn.SelectAllInt16("select * from (values ($1), ($2)) t", 1, 2)
	if err != nil {
		t.Fatal("Unable to select all int16: " + err.Error())
	}

	if i[0] != 1 || i[1] != 2 {
		t.Error("Received incorrect int16")
	}

	i, err = conn.SelectAllInt16("select power(2,17)::numeric")
	if err == nil || !strings.Contains(err.Error(), "value out of range") {
		t.Error("Expected value out of range error when selecting number greater than max int16")
	}

	i, err = conn.SelectAllInt16("select -power(2,17)::numeric")
	if err == nil || !strings.Contains(err.Error(), "value out of range") {
		t.Error("Expected value out of range error when selecting number less than min int16")
	}
}

func TestSelectAllFloat64(t *testing.T) {
	conn := getSharedConn()

	f, err := conn.SelectAllFloat64("select * from (values ($1), ($2)) t", 1.23, 4.56)
	if err != nil {
		t.Fatal("Unable to select all float64: " + err.Error())
	}

	if f[0] != 1.23 || f[1] != 4.56 {
		t.Error("Received incorrect float64")
	}
}

func TestSelectAllFloat32(t *testing.T) {
	conn := getSharedConn()

	f, err := conn.SelectAllFloat32("select * from (values ($1), ($2)) t", 1.23, 4.56)
	if err != nil {
		t.Fatal("Unable to select all float32: " + err.Error())
	}

	if f[0] != 1.23 || f[1] != 4.56 {
		t.Error("Received incorrect float32")
	}
}
