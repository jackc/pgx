package pgx

import (
	"testing"
)

func TestDataRowReaderReadValue(t *testing.T) {
	conn := getSharedConnection()
	var v interface{}
	var err error

	onDataRow := func(r *DataRowReader) error {
		v = r.ReadValue()
		return nil
	}

	err = conn.SelectFunc("select null", onDataRow)
	if err != nil {
		t.Fatalf("Select failed: %v", err)
	}
	if v != nil {
		t.Errorf("Wrong value returned: %v", v)
	}

	err = conn.SelectFunc("select 'Jack'", onDataRow)
	if err != nil {
		t.Fatalf("Select failed: %v", err)
	}
	if typedValue, _ := v.(string); typedValue != "Jack" {
		t.Errorf("Wrong value returned: %v", typedValue)
	}

	err = conn.SelectFunc("select true", onDataRow)
	if err != nil {
		t.Errorf("Unable to select bool: %#v", err)
	} else {
		s, ok := v.(bool)
		if !(ok && s == true) {
			t.Errorf("Expected true, received: %#v", v)
		}
	}

	err = conn.SelectFunc("select false", onDataRow)
	if err != nil {
		t.Errorf("Unable to select bool: %v", err)
	} else {
		s, ok := v.(bool)
		if !(ok && s == false) {
			t.Errorf("Expected false, received: %#v", v)
		}
	}

	err = conn.SelectFunc("select 1::int2", onDataRow)
	if err != nil {
		t.Errorf("Unable to select int2: %v", err)
	} else {
		s, ok := v.(int16)
		if !(ok && s == 1) {
			t.Errorf("Expected 1, received: %#v", v)
		}
	}

	err = conn.SelectFunc("select 1::int4", onDataRow)
	if err != nil {
		t.Errorf("Unable to select int4: %v", err)
	} else {
		s, ok := v.(int32)
		if !(ok && s == 1) {
			t.Errorf("Expected 1, received: %#v", v)
		}
	}

	err = conn.SelectFunc("select 1::int8", onDataRow)
	if err != nil {
		t.Errorf("Unable to select int8: %#v", err)
	} else {
		s, ok := v.(int64)
		if !(ok && s == 1) {
			t.Errorf("Expected 1, received: %#v", v)
		}
	}

	err = conn.SelectFunc("select 1.23::float4", onDataRow)
	if err != nil {
		t.Errorf("Unable to select float4: %#v", err)
	} else {
		s, ok := v.(float32)
		if !(ok && s == float32(1.23)) {
			t.Errorf("Expected 1.23, received: %#v", v)
		}
	}
}
