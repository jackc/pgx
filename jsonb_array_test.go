package pgtype_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgtype/testutil"
)

func TestJSONBArrayTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscode(t, "jsonb[]", []interface{}{
		&pgtype.JSONBArray{
			Elements:   nil,
			Dimensions: nil,
			Status:     pgtype.Present,
		},
		&pgtype.JSONBArray{
			Elements: []pgtype.JSONB{
				{Bytes: []byte(`"foo"`), Status: pgtype.Present},
				{Status: pgtype.Null},
			},
			Dimensions: []pgtype.ArrayDimension{{Length: 2, LowerBound: 1}},
			Status:     pgtype.Present,
		},
		&pgtype.JSONBArray{Status: pgtype.Null},
		&pgtype.JSONBArray{
			Elements: []pgtype.JSONB{
				{Bytes: []byte(`"foo"`), Status: pgtype.Present},
				{Bytes: []byte("null"), Status: pgtype.Present},
				{Bytes: []byte("42"), Status: pgtype.Present},
			},
			Dimensions: []pgtype.ArrayDimension{{Length: 3, LowerBound: 1}},
			Status:     pgtype.Present,
		},
	})
}

func TestJSONBArraySet(t *testing.T) {
	successfulTests := []struct {
		source interface{}
		result pgtype.JSONBArray
	}{
		{source: []string{"{}"}, result: pgtype.JSONBArray{
			Elements:   []pgtype.JSONB{pgtype.JSONB{Bytes: []byte("{}"), Status: pgtype.Present}},
			Dimensions: []pgtype.ArrayDimension{pgtype.ArrayDimension{Length: 1, LowerBound: 1}},
			Status:     pgtype.Present,
		}},
		{source: [][]byte{[]byte("{}")}, result: pgtype.JSONBArray{
			Elements:   []pgtype.JSONB{pgtype.JSONB{Bytes: []byte("{}"), Status: pgtype.Present}},
			Dimensions: []pgtype.ArrayDimension{pgtype.ArrayDimension{Length: 1, LowerBound: 1}},
			Status:     pgtype.Present,
		}},
		{source: [][]byte{[]byte(`{"foo":1}`), []byte(`{"bar":2}`)}, result: pgtype.JSONBArray{
			Elements:   []pgtype.JSONB{pgtype.JSONB{Bytes: []byte(`{"foo":1}`), Status: pgtype.Present}, pgtype.JSONB{Bytes: []byte(`{"bar":2}`), Status: pgtype.Present}},
			Dimensions: []pgtype.ArrayDimension{pgtype.ArrayDimension{Length: 2, LowerBound: 1}},
			Status:     pgtype.Present,
		}},
		{source: []json.RawMessage{json.RawMessage(`{"foo":1}`), json.RawMessage(`{"bar":2}`)}, result: pgtype.JSONBArray{
			Elements:   []pgtype.JSONB{pgtype.JSONB{Bytes: []byte(`{"foo":1}`), Status: pgtype.Present}, pgtype.JSONB{Bytes: []byte(`{"bar":2}`), Status: pgtype.Present}},
			Dimensions: []pgtype.ArrayDimension{pgtype.ArrayDimension{Length: 2, LowerBound: 1}},
			Status:     pgtype.Present,
		}},
		{source: []json.RawMessage{json.RawMessage(`{"foo":12}`), json.RawMessage(`{"bar":2}`)}, result: pgtype.JSONBArray{
			Elements:   []pgtype.JSONB{pgtype.JSONB{Bytes: []byte(`{"foo":12}`), Status: pgtype.Present}, pgtype.JSONB{Bytes: []byte(`{"bar":2}`), Status: pgtype.Present}},
			Dimensions: []pgtype.ArrayDimension{pgtype.ArrayDimension{Length: 2, LowerBound: 1}},
			Status:     pgtype.Present,
		}},
		{source: []json.RawMessage{json.RawMessage(`{"foo":1}`), json.RawMessage(`{"bar":{"x":2}}`)}, result: pgtype.JSONBArray{
			Elements:   []pgtype.JSONB{pgtype.JSONB{Bytes: []byte(`{"foo":1}`), Status: pgtype.Present}, pgtype.JSONB{Bytes: []byte(`{"bar":{"x":2}}`), Status: pgtype.Present}},
			Dimensions: []pgtype.ArrayDimension{pgtype.ArrayDimension{Length: 2, LowerBound: 1}},
			Status:     pgtype.Present,
		}},
	}

	for i, tt := range successfulTests {
		var d pgtype.JSONBArray
		err := d.Set(tt.source)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if !reflect.DeepEqual(d, tt.result) {
			t.Errorf("%d: expected %+v to convert to %+v, but it was %+v", i, tt.source, tt.result, d)
		}
	}
}
