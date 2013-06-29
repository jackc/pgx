package pgx

import (
	"testing"
)

func TestSelectValue(t *testing.T) {
	conn := getSharedConnection()
	var v interface{}
	var err error

	v, err = conn.SelectValue("select null")
	if err != nil {
		t.Errorf("Unable to select null: %v", err)
	} else {
		if v != nil {
			t.Errorf("Expected: nil, recieved: %v", v)
		}
	}

	v, err = conn.SelectValue("select 'foo'")
	if err != nil {
		t.Errorf("Unable to select string: %v", err)
	} else {
		s, ok := v.(string)
		if !(ok && s == "foo") {
			t.Errorf("Expected: foo, recieved: %#v", v)
		}
	}

	v, err = conn.SelectValue("select true")
	if err != nil {
		t.Errorf("Unable to select bool: %#v", err)
	} else {
		s, ok := v.(bool)
		if !(ok && s == true) {
			t.Errorf("Expected true, received: %#v", v)
		}
	}

	v, err = conn.SelectValue("select false")
	if err != nil {
		t.Errorf("Unable to select bool: %v", err)
	} else {
		s, ok := v.(bool)
		if !(ok && s == false) {
			t.Errorf("Expected false, received: %#v", v)
		}
	}

	v, err = conn.SelectValue("select 1::int2")
	if err != nil {
		t.Errorf("Unable to select int2: %v", err)
	} else {
		s, ok := v.(int16)
		if !(ok && s == 1) {
			t.Errorf("Expected 1, received: %#v", v)
		}
	}

	v, err = conn.SelectValue("select 1::int4")
	if err != nil {
		t.Errorf("Unable to select int4: %v", err)
	} else {
		s, ok := v.(int32)
		if !(ok && s == 1) {
			t.Errorf("Expected 1, received: %#v", v)
		}
	}

	v, err = conn.SelectValue("select 1::int8")
	if err != nil {
		t.Errorf("Unable to select int8: %#v", err)
	} else {
		s, ok := v.(int64)
		if !(ok && s == 1) {
			t.Errorf("Expected 1, received: %#v", v)
		}
	}

	v, err = conn.SelectValue("select 1.23::float4")
	if err != nil {
		t.Errorf("Unable to select float4: %#v", err)
	} else {
		s, ok := v.(float32)
		if !(ok && s == float32(1.23)) {
			t.Errorf("Expected 1.23, received: %#v", v)
		}
	}
}
