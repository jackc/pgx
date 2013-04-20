package pgx

import (
	"strings"
	"testing"
)

func TestSelectAllString(t *testing.T) {
	conn := getSharedConnection()

	s, err := conn.SelectAllString("select * from (values ('Matthew'), ('Mark'), ('Luke'), ('John')) t")
	if err != nil {
		t.Fatal("Unable to select all strings: " + err.Error())
	}

	if s[0] != "Matthew" || s[1] != "Mark" || s[2] != "Luke" || s[3] != "John" {
		t.Error("Received incorrect strings")
	}

	_, err = conn.SelectAllString("select * from (values ('Matthew'), (null)) t")
	if err == nil || !strings.Contains(err.Error(), "NULL") {
		t.Error("Should have received error on null")
	}
}

func TestSelectAllInt64(t *testing.T) {
	conn := getSharedConnection()

	i, err := conn.SelectAllInt64("select * from (values (1), (2)) t")
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

	_, err = conn.SelectAllInt64("select null")
	if err == nil || !strings.Contains(err.Error(), "NULL") {
		t.Error("Should have received error on null")
	}
}

func TestSelectAllInt32(t *testing.T) {
	conn := getSharedConnection()

	i, err := conn.SelectAllInt32("select * from (values (1), (2)) t")
	if err != nil {
		t.Fatal("Unable to select all int32: " + err.Error())
	}

	if i[0] != 1 || i[1] != 2 {
		t.Error("Received incorrect int32s")
	}

	i, err = conn.SelectAllInt32("select power(2,33)::numeric")
	if err == nil || !strings.Contains(err.Error(), "value out of range") {
		t.Error("Expected value out of range error when selecting number greater than max int32")
	}

	i, err = conn.SelectAllInt32("select -power(2,33)::numeric")
	if err == nil || !strings.Contains(err.Error(), "value out of range") {
		t.Error("Expected value out of range error when selecting number less than min int32")
	}

	_, err = conn.SelectAllInt32("select null")
	if err == nil || !strings.Contains(err.Error(), "NULL") {
		t.Error("Should have received error on null")
	}
}

func TestSelectAllInt16(t *testing.T) {
	conn := getSharedConnection()

	i, err := conn.SelectAllInt16("select * from (values (1), (2)) t")
	if err != nil {
		t.Fatal("Unable to select all int16: " + err.Error())
	}

	if i[0] != 1 || i[1] != 2 {
		t.Error("Received incorrect int16s")
	}

	i, err = conn.SelectAllInt16("select power(2,17)::numeric")
	if err == nil || !strings.Contains(err.Error(), "value out of range") {
		t.Error("Expected value out of range error when selecting number greater than max int16")
	}

	i, err = conn.SelectAllInt16("select -power(2,17)::numeric")
	if err == nil || !strings.Contains(err.Error(), "value out of range") {
		t.Error("Expected value out of range error when selecting number less than min int16")
	}

	_, err = conn.SelectAllInt16("select null")
	if err == nil || !strings.Contains(err.Error(), "NULL") {
		t.Error("Should have received error on null")
	}
}

func TestSelectAllFloat64(t *testing.T) {
	conn := getSharedConnection()

	f, err := conn.SelectAllFloat64("select * from (values (1.23), (4.56)) t")
	if err != nil {
		t.Fatal("Unable to select all float64: " + err.Error())
	}

	if f[0] != 1.23 || f[1] != 4.56 {
		t.Error("Received incorrect float64")
	}

	_, err = conn.SelectAllFloat64("select null")
	if err == nil || !strings.Contains(err.Error(), "NULL") {
		t.Error("Should have received error on null")
	}
}

func TestSelectAllFloat32(t *testing.T) {
	conn := getSharedConnection()

	f, err := conn.SelectAllFloat32("select * from (values (1.23), (4.56)) t")
	if err != nil {
		t.Fatal("Unable to select all float32: " + err.Error())
	}

	if f[0] != 1.23 || f[1] != 4.56 {
		t.Error("Received incorrect float32")
	}

	_, err = conn.SelectAllFloat32("select null")
	if err == nil || !strings.Contains(err.Error(), "NULL") {
		t.Error("Should have received error on null")
	}
}
