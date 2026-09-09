package pgtype

import (
	"reflect"
	"testing"
)

func TestParseUntypedTextMultirangeBounds(t *testing.T) {
	for _, tt := range []struct {
		src  string
		want []string
	}{
		{`{}`, []string{}},
		{`{empty}`, []string{`empty`}},
		{`{[a,z)}`, []string{`[a,z)`}},
		{`{["a,b",z)}`, []string{`["a,b",z)`}},
		{`{[a}b,z)}`, []string{`[a}b,z)`}},
		{`{["a)b",z)}`, []string{`["a)b",z)`}},
		{`{["a""b,c",z)}`, []string{`["a""b,c",z)`}},
		{`{[a\,b,z)}`, []string{`[a\,b,z)`}},
		{`{[a\)b,z)}`, []string{`[a\)b,z)`}},
		{`{["a,b",c),[d,"e,f"]}`, []string{`["a,b",c)`, `[d,"e,f"]`}},
	} {
		t.Run(tt.src, func(t *testing.T) {
			got, err := parseUntypedTextMultirange([]byte(tt.src))
			if err != nil || !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("got %q, %v; want %q", got, err, tt.want)
			}
		})
	}
	for _, src := range []string{`{["a,b",z)`, `{["a,b,z)}`, `{[a,z)\\`, `{[a,z)}trailing`} {
		t.Run(src, func(t *testing.T) {
			if _, err := parseUntypedTextMultirange([]byte(src)); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}
