package pgx

import (
	"strings"
	"testing"
)

func TestSelectString(t *testing.T) {
	conn := getSharedConnection()

	s, err := conn.SelectString("select 'foo'")
	if err != nil {
		t.Error("Unable to select string: " + err.Error())
	} else if s != "foo" {
		t.Error("Received incorrect string")
	}

	_, err = conn.SelectString("select null")
	if err == nil {
		t.Error("Should have received error on null")
	}
}


func TestSelectInt64(t *testing.T) {
	conn := getSharedConnection()

	i, err := conn.SelectInt64("select 1")
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

	_, err = conn.SelectInt64("select null")
	if err == nil || !strings.Contains(err.Error(), "NULL") {
		t.Error("Should have received error on null")
	}
}

func TestSelectInt32(t *testing.T) {
	conn := getSharedConnection()

	i, err := conn.SelectInt32("select 1")
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

	_, err = conn.SelectInt32("select null")
	if err == nil || !strings.Contains(err.Error(), "NULL") {
		t.Error("Should have received error on null")
	}
}

func TestSelectInt16(t *testing.T) {
	conn := getSharedConnection()

	i, err := conn.SelectInt16("select 1")
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

	_, err = conn.SelectInt16("select null")
	if err == nil || !strings.Contains(err.Error(), "NULL") {
		t.Error("Should have received error on null")
	}
}



func TestSelectFloat64(t *testing.T) {
	conn := getSharedConnection()

	f, err := conn.SelectFloat64("select 1.23")
	if err != nil {
		t.Fatal("Unable to select float64: " + err.Error())
	}

	if f != 1.23 {
		t.Error("Received incorrect float64")
	}

	_, err = conn.SelectFloat64("select null")
	if err == nil || !strings.Contains(err.Error(), "NULL") {
		t.Error("Should have received error on null")
	}
}

func TestSelectFloat32(t *testing.T) {
	conn := getSharedConnection()

	f, err := conn.SelectFloat32("select 1.23")
	if err != nil {
		t.Fatal("Unable to select float32: " + err.Error())
	}

	if f != 1.23 {
		t.Error("Received incorrect float32")
	}

	_, err = conn.SelectFloat32("select null")
	if err == nil || !strings.Contains(err.Error(), "NULL") {
		t.Error("Should have received error on null")
	}
}

