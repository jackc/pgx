package pgx_test

import (
	"testing"
	"time"
)

func TestNilTranscode(t *testing.T) {
	conn := getSharedConnection(t)

	var inputNil interface{}
	inputNil = nil

	result := mustSelectValue(t, conn, "select $1::integer", inputNil)
	if result != nil {
		t.Errorf("Did not transcode nil successfully for normal query: %v", result)
	}

	mustPrepare(t, conn, "testTranscode", "select $1::integer")
	defer func() {
		if err := conn.Deallocate("testTranscode"); err != nil {
			t.Fatalf("Unable to deallocate prepared statement: %v", err)
		}
	}()

	result = mustSelectValue(t, conn, "testTranscode", inputNil)
	if result != nil {
		t.Errorf("Did not transcode nil successfully for prepared query: %v", result)
	}
}

func TestDateTranscode(t *testing.T) {
	conn := getSharedConnection(t)

	actualDate := time.Date(2013, 1, 2, 0, 0, 0, 0, time.Local)

	var v interface{}
	var d time.Time

	v = mustSelectValue(t, conn, "select $1::date", actualDate)
	d = v.(time.Time)
	if !actualDate.Equal(d) {
		t.Errorf("Did not transcode date successfully: %v is not %v", v, actualDate)
	}

	mustPrepare(t, conn, "testTranscode", "select $1::date")
	defer func() {
		if err := conn.Deallocate("testTranscode"); err != nil {
			t.Fatalf("Unable to deallocate prepared statement: %v", err)
		}
	}()

	v = mustSelectValue(t, conn, "testTranscode", actualDate)
	d = v.(time.Time)
	if !actualDate.Equal(d) {
		t.Errorf("Did not transcode date successfully: %v is not %v", v, actualDate)
	}
}

func TestTimestampTzTranscode(t *testing.T) {
	conn := getSharedConnection(t)

	inputTime := time.Date(2013, 1, 2, 3, 4, 5, 6000, time.Local)

	var v interface{}
	var outputTime time.Time

	v = mustSelectValue(t, conn, "select $1::timestamptz", inputTime)
	outputTime = v.(time.Time)
	if !inputTime.Equal(outputTime) {
		t.Errorf("Did not transcode time successfully: %v is not %v", outputTime, inputTime)
	}

	mustPrepare(t, conn, "testTranscode", "select $1::timestamptz")
	defer func() {
		if err := conn.Deallocate("testTranscode"); err != nil {
			t.Fatalf("Unable to deallocate prepared statement: %v", err)
		}
	}()

	v = mustSelectValue(t, conn, "testTranscode", inputTime)
	outputTime = v.(time.Time)
	if !inputTime.Equal(outputTime) {
		t.Errorf("Did not transcode time successfully: %v is not %v", outputTime, inputTime)
	}
}

func TestInt2SliceTranscode(t *testing.T) {
	testEqual := func(a, b []int16) {
		if len(a) != len(b) {
			t.Errorf("Did not transcode []int16 successfully: %v is not %v", a, b)
		}
		for i := range a {
			if a[i] != b[i] {
				t.Errorf("Did not transcode []int16 successfully: %v is not %v", a, b)
			}
		}
	}

	conn := getSharedConnection(t)

	inputNumbers := []int16{1, 2, 3, 4, 5, 6, 7, 8}
	var outputNumbers []int16

	outputNumbers = mustSelectValue(t, conn, "select $1::int2[]", inputNumbers).([]int16)
	testEqual(inputNumbers, outputNumbers)

	mustPrepare(t, conn, "testTranscode", "select $1::int2[]")
	defer func() {
		if err := conn.Deallocate("testTranscode"); err != nil {
			t.Fatalf("Unable to deallocate prepared statement: %v", err)
		}
	}()

	outputNumbers = mustSelectValue(t, conn, "testTranscode", inputNumbers).([]int16)
	testEqual(inputNumbers, outputNumbers)
}

func TestInt4SliceTranscode(t *testing.T) {
	testEqual := func(a, b []int32) {
		if len(a) != len(b) {
			t.Errorf("Did not transcode []int32 successfully: %v is not %v", a, b)
		}
		for i := range a {
			if a[i] != b[i] {
				t.Errorf("Did not transcode []int32 successfully: %v is not %v", a, b)
			}
		}
	}

	conn := getSharedConnection(t)

	inputNumbers := []int32{1, 2, 3, 4, 5, 6, 7, 8}
	var outputNumbers []int32

	outputNumbers = mustSelectValue(t, conn, "select $1::int4[]", inputNumbers).([]int32)
	testEqual(inputNumbers, outputNumbers)

	mustPrepare(t, conn, "testTranscode", "select $1::int4[]")
	defer func() {
		if err := conn.Deallocate("testTranscode"); err != nil {
			t.Fatalf("Unable to deallocate prepared statement: %v", err)
		}
	}()

	outputNumbers = mustSelectValue(t, conn, "testTranscode", inputNumbers).([]int32)
	testEqual(inputNumbers, outputNumbers)
}

func TestInt8SliceTranscode(t *testing.T) {
	testEqual := func(a, b []int64) {
		if len(a) != len(b) {
			t.Errorf("Did not transcode []int64 successfully: %v is not %v", a, b)
		}
		for i := range a {
			if a[i] != b[i] {
				t.Errorf("Did not transcode []int64 successfully: %v is not %v", a, b)
			}
		}
	}

	conn := getSharedConnection(t)

	inputNumbers := []int64{1, 2, 3, 4, 5, 6, 7, 8}
	var outputNumbers []int64

	outputNumbers = mustSelectValue(t, conn, "select $1::int8[]", inputNumbers).([]int64)
	testEqual(inputNumbers, outputNumbers)

	mustPrepare(t, conn, "testTranscode", "select $1::int8[]")
	defer func() {
		if err := conn.Deallocate("testTranscode"); err != nil {
			t.Fatalf("Unable to deallocate prepared statement: %v", err)
		}
	}()

	outputNumbers = mustSelectValue(t, conn, "testTranscode", inputNumbers).([]int64)
	testEqual(inputNumbers, outputNumbers)
}
