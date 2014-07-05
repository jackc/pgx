package pgx_test

import (
	"testing"
	"time"
)

func TestEncodeError(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	mustPrepare(t, conn, "testTranscode", "select $1::integer")
	defer func() {
		if err := conn.Deallocate("testTranscode"); err != nil {
			t.Fatalf("Unable to deallocate prepared statement: %v", err)
		}
	}()

	_, err := conn.Query("testTranscode", "wrong")
	switch {
	case err == nil:
		t.Error("Expected transcode error to return error, but it didn't")
	case err.Error() == "Expected integer representable in int32, received string wrong":
		// Correct behavior
	default:
		t.Errorf("Expected transcode error, received %v", err)
	}
}

// TODO
func TestNilTranscode(t *testing.T) {
	// t.Parallel()

	// conn := mustConnect(t, *defaultConnConfig)
	// defer closeConn(t, conn)

	// var inputNil interface{}
	// inputNil = nil

	// result := mustSelectValue(t, conn, "select $1::integer", inputNil)
	// if result != nil {
	// 	t.Errorf("Did not transcode nil successfully for normal query: %v", result)
	// }

	// mustPrepare(t, conn, "testTranscode", "select $1::integer")
	// defer func() {
	// 	if err := conn.Deallocate("testTranscode"); err != nil {
	// 		t.Fatalf("Unable to deallocate prepared statement: %v", err)
	// 	}
	// }()

	// result = mustSelectValue(t, conn, "testTranscode", inputNil)
	// if result != nil {
	// 	t.Errorf("Did not transcode nil successfully for prepared query: %v", result)
	// }
}

func TestDateTranscode(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	mustPrepare(t, conn, "testTranscode", "select $1::date")
	defer func() {
		if err := conn.Deallocate("testTranscode"); err != nil {
			t.Fatalf("Unable to deallocate prepared statement: %v", err)
		}
	}()

	dates := []time.Time{
		time.Date(1990, 1, 1, 0, 0, 0, 0, time.Local),
		time.Date(1999, 12, 31, 0, 0, 0, 0, time.Local),
		time.Date(2000, 1, 1, 0, 0, 0, 0, time.Local),
		time.Date(2001, 1, 2, 0, 0, 0, 0, time.Local),
		time.Date(2004, 2, 29, 0, 0, 0, 0, time.Local),
		time.Date(2013, 7, 4, 0, 0, 0, 0, time.Local),
		time.Date(2013, 12, 25, 0, 0, 0, 0, time.Local),
	}

	for _, actualDate := range dates {
		var d time.Time

		// Test text format
		err := conn.QueryRow("select $1::date", actualDate).Scan(&d)
		if err != nil {
			t.Fatalf("Unexpected failure on QueryRow Scan: %v", err)
		}
		if !actualDate.Equal(d) {
			t.Errorf("Did not transcode date successfully: %v is not %v", d, actualDate)
		}

		// Test binary format
		err = conn.QueryRow("testTranscode", actualDate).Scan(&d)
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

	mustPrepare(t, conn, "testTranscode", "select $1::timestamptz")
	defer func() {
		if err := conn.Deallocate("testTranscode"); err != nil {
			t.Fatalf("Unable to deallocate prepared statement: %v", err)
		}
	}()

	err = conn.QueryRow("testTranscode", inputTime).Scan(&outputTime)
	if err != nil {
		t.Fatalf("QueryRow Scan failed: %v", err)
	}
	if !inputTime.Equal(outputTime) {
		t.Errorf("Did not transcode time successfully: %v is not %v", outputTime, inputTime)
	}
}

// func TestInt2SliceTranscode(t *testing.T) {
// 	t.Parallel()

// 	testEqual := func(a, b []int16) {
// 		if len(a) != len(b) {
// 			t.Errorf("Did not transcode []int16 successfully: %v is not %v", a, b)
// 		}
// 		for i := range a {
// 			if a[i] != b[i] {
// 				t.Errorf("Did not transcode []int16 successfully: %v is not %v", a, b)
// 			}
// 		}
// 	}

// 	conn := mustConnect(t, *defaultConnConfig)
// 	defer closeConn(t, conn)

// 	inputNumbers := []int16{1, 2, 3, 4, 5, 6, 7, 8}
// 	var outputNumbers []int16

// 	outputNumbers = mustSelectValue(t, conn, "select $1::int2[]", inputNumbers).([]int16)
// 	testEqual(inputNumbers, outputNumbers)

// 	mustPrepare(t, conn, "testTranscode", "select $1::int2[]")
// 	defer func() {
// 		if err := conn.Deallocate("testTranscode"); err != nil {
// 			t.Fatalf("Unable to deallocate prepared statement: %v", err)
// 		}
// 	}()

// 	outputNumbers = mustSelectValue(t, conn, "testTranscode", inputNumbers).([]int16)
// 	testEqual(inputNumbers, outputNumbers)
// }

// func TestInt4SliceTranscode(t *testing.T) {
// 	t.Parallel()

// 	testEqual := func(a, b []int32) {
// 		if len(a) != len(b) {
// 			t.Errorf("Did not transcode []int32 successfully: %v is not %v", a, b)
// 		}
// 		for i := range a {
// 			if a[i] != b[i] {
// 				t.Errorf("Did not transcode []int32 successfully: %v is not %v", a, b)
// 			}
// 		}
// 	}

// 	conn := mustConnect(t, *defaultConnConfig)
// 	defer closeConn(t, conn)

// 	inputNumbers := []int32{1, 2, 3, 4, 5, 6, 7, 8}
// 	var outputNumbers []int32

// 	outputNumbers = mustSelectValue(t, conn, "select $1::int4[]", inputNumbers).([]int32)
// 	testEqual(inputNumbers, outputNumbers)

// 	mustPrepare(t, conn, "testTranscode", "select $1::int4[]")
// 	defer func() {
// 		if err := conn.Deallocate("testTranscode"); err != nil {
// 			t.Fatalf("Unable to deallocate prepared statement: %v", err)
// 		}
// 	}()

// 	outputNumbers = mustSelectValue(t, conn, "testTranscode", inputNumbers).([]int32)
// 	testEqual(inputNumbers, outputNumbers)
// }

// func TestInt8SliceTranscode(t *testing.T) {
// 	t.Parallel()

// 	testEqual := func(a, b []int64) {
// 		if len(a) != len(b) {
// 			t.Errorf("Did not transcode []int64 successfully: %v is not %v", a, b)
// 		}
// 		for i := range a {
// 			if a[i] != b[i] {
// 				t.Errorf("Did not transcode []int64 successfully: %v is not %v", a, b)
// 			}
// 		}
// 	}

// 	conn := mustConnect(t, *defaultConnConfig)
// 	defer closeConn(t, conn)

// 	inputNumbers := []int64{1, 2, 3, 4, 5, 6, 7, 8}
// 	var outputNumbers []int64

// 	outputNumbers = mustSelectValue(t, conn, "select $1::int8[]", inputNumbers).([]int64)
// 	testEqual(inputNumbers, outputNumbers)

// 	mustPrepare(t, conn, "testTranscode", "select $1::int8[]")
// 	defer func() {
// 		if err := conn.Deallocate("testTranscode"); err != nil {
// 			t.Fatalf("Unable to deallocate prepared statement: %v", err)
// 		}
// 	}()

// 	outputNumbers = mustSelectValue(t, conn, "testTranscode", inputNumbers).([]int64)
// 	testEqual(inputNumbers, outputNumbers)
// }
