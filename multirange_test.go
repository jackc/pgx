package pgtype

import (
	"reflect"
	"testing"
)

func TestParseUntypedTextMultirange(t *testing.T) {
	tests := []struct {
		src    string
		result UntypedTextMultirange
		err    error
	}{
		{
			src:    `{[1,2)}`,
			result: UntypedTextMultirange{Elements: []string{`[1,2)`}},
			err:    nil,
		},
		{
			src:    `{[,),["foo", "bar"]}`,
			result: UntypedTextMultirange{Elements: []string{`[,)`, `["foo", "bar"]`}},
			err:    nil,
		},
		{
			src:    `{}`,
			result: UntypedTextMultirange{Elements: []string{}},
			err:    nil,
		},
		{
			src:    ` { (,) , [1,2] } `,
			result: UntypedTextMultirange{Elements: []string{` (,) `, ` [1,2] `}},
			err:    nil,
		},
		{
			src:    `{["f""oo","b""ar")}`,
			result: UntypedTextMultirange{Elements: []string{`["f""oo","b""ar")`}},
			err:    nil,
		},
	}
	for i, tt := range tests {
		r, err := ParseUntypedTextMultirange(tt.src)
		if err != tt.err {
			t.Errorf("%d. `%v`: expected err %v, got %v", i, tt.src, tt.err, err)
			continue
		}

		if !reflect.DeepEqual(*r, tt.result) {
			t.Errorf("%d: expected %+v to be parsed to %+v, but it was %+v", i, tt.src, tt.result, *r)
		}
	}
}
