package pgx_test

import (
	"testing"
	"time"
)

func TestDateTranscode(t *testing.T) {
	conn := getSharedConnection()

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
	conn := getSharedConnection()

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
