package pgx

import (
	"testing"
)

func TestDataRowReaderReadString(t *testing.T) {
	conn := getSharedConnection()

	var s string
	onDataRow := func(r *DataRowReader) error {
		s = r.ReadString()
		return nil
	}

	err := conn.SelectFunc("select 'Jack'", onDataRow)
	if err != nil {
		t.Fatal("Select failed: " + err.Error())
	}
	if s != "Jack" {
		t.Error("Wrong value returned")
	}
}

func TestDataRowReaderReadInt64(t *testing.T) {
	conn := getSharedConnection()

	var n int64
	onDataRow := func(r *DataRowReader) error {
		n = r.ReadInt64()
		return nil
	}

	err := conn.SelectFunc("select 1", onDataRow)
	if err != nil {
		t.Fatal("Select failed: " + err.Error())
	}
	if n != 1 {
		t.Error("Wrong value returned")
	}
}

func TestDataRowReaderReadInt32(t *testing.T) {
	conn := getSharedConnection()

	var n int32
	onDataRow := func(r *DataRowReader) error {
		n = r.ReadInt32()
		return nil
	}

	err := conn.SelectFunc("select 1", onDataRow)
	if err != nil {
		t.Fatal("Select failed: " + err.Error())
	}
	if n != 1 {
		t.Error("Wrong value returned")
	}
}

func TestDataRowReaderReadInt16(t *testing.T) {
	conn := getSharedConnection()

	var n int16
	onDataRow := func(r *DataRowReader) error {
		n = r.ReadInt16()
		return nil
	}

	err := conn.SelectFunc("select 1", onDataRow)
	if err != nil {
		t.Fatal("Select failed: " + err.Error())
	}
	if n != 1 {
		t.Error("Wrong value returned")
	}
}


func TestDataRowReaderReadFloat64(t *testing.T) {
	conn := getSharedConnection()

	var n float64
	onDataRow := func(r *DataRowReader) error {
		n = r.ReadFloat64()
		return nil
	}

	err := conn.SelectFunc("select 1.5", onDataRow)
	if err != nil {
		t.Fatal("Select failed: " + err.Error())
	}
	if n != 1.5 {
		t.Error("Wrong value returned")
	}
}

func TestDataRowReaderReadFloat32(t *testing.T) {
	conn := getSharedConnection()

	var n float32
	onDataRow := func(r *DataRowReader) error {
		n = r.ReadFloat32()
		return nil
	}

	err := conn.SelectFunc("select 1.5", onDataRow)
	if err != nil {
		t.Fatal("Select failed: " + err.Error())
	}
	if n != 1.5 {
		t.Error("Wrong value returned")
	}
}

