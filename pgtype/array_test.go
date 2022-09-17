package pgtype

import (
	"reflect"
	"testing"
)

func TestParseUntypedTextArray(t *testing.T) {
	tests := []struct {
		source string
		result untypedTextArray
	}{
		{
			source: "{}",
			result: untypedTextArray{
				Elements:   []string{},
				Quoted:     []bool{},
				Dimensions: []ArrayDimension{},
			},
		},
		{
			source: "{1}",
			result: untypedTextArray{
				Elements:   []string{"1"},
				Quoted:     []bool{false},
				Dimensions: []ArrayDimension{{Length: 1, LowerBound: 1}},
			},
		},
		{
			source: "{a,b}",
			result: untypedTextArray{
				Elements:   []string{"a", "b"},
				Quoted:     []bool{false, false},
				Dimensions: []ArrayDimension{{Length: 2, LowerBound: 1}},
			},
		},
		{
			source: `{"NULL"}`,
			result: untypedTextArray{
				Elements:   []string{"NULL"},
				Quoted:     []bool{true},
				Dimensions: []ArrayDimension{{Length: 1, LowerBound: 1}},
			},
		},
		{
			source: `{""}`,
			result: untypedTextArray{
				Elements:   []string{""},
				Quoted:     []bool{true},
				Dimensions: []ArrayDimension{{Length: 1, LowerBound: 1}},
			},
		},
		{
			source: `{"He said, \"Hello.\""}`,
			result: untypedTextArray{
				Elements:   []string{`He said, "Hello."`},
				Quoted:     []bool{true},
				Dimensions: []ArrayDimension{{Length: 1, LowerBound: 1}},
			},
		},
		{
			source: "{{a,b},{c,d},{e,f}}",
			result: untypedTextArray{
				Elements:   []string{"a", "b", "c", "d", "e", "f"},
				Quoted:     []bool{false, false, false, false, false, false},
				Dimensions: []ArrayDimension{{Length: 3, LowerBound: 1}, {Length: 2, LowerBound: 1}},
			},
		},
		{
			source: "{{{a,b},{c,d},{e,f}},{{a,b},{c,d},{e,f}}}",
			result: untypedTextArray{
				Elements: []string{"a", "b", "c", "d", "e", "f", "a", "b", "c", "d", "e", "f"},
				Quoted:   []bool{false, false, false, false, false, false, false, false, false, false, false, false},
				Dimensions: []ArrayDimension{
					{Length: 2, LowerBound: 1},
					{Length: 3, LowerBound: 1},
					{Length: 2, LowerBound: 1},
				},
			},
		},
		{
			source: "[4:4]={1}",
			result: untypedTextArray{
				Elements:   []string{"1"},
				Quoted:     []bool{false},
				Dimensions: []ArrayDimension{{Length: 1, LowerBound: 4}},
			},
		},
		{
			source: "[4:5][2:3]={{a,b},{c,d}}",
			result: untypedTextArray{
				Elements: []string{"a", "b", "c", "d"},
				Quoted:   []bool{false, false, false, false},
				Dimensions: []ArrayDimension{
					{Length: 2, LowerBound: 4},
					{Length: 2, LowerBound: 2},
				},
			},
		},
		{
			source: "[-4:-2]={1,2,3}",
			result: untypedTextArray{
				Elements:   []string{"1", "2", "3"},
				Quoted:     []bool{false, false, false},
				Dimensions: []ArrayDimension{{Length: 3, LowerBound: -4}},
			},
		},
	}

	for i, tt := range tests {
		r, err := parseUntypedTextArray(tt.source)
		if err != nil {
			t.Errorf("%d: %v", i, err)
			continue
		}

		if !reflect.DeepEqual(*r, tt.result) {
			t.Errorf("%d: expected %+v to be parsed to %+v, but it was %+v", i, tt.source, tt.result, *r)
		}
	}
}
