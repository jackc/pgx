package pgx

import (
	"reflect"
	"testing"
)

func TestEscapeAclItem(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{
			"foo",
			"foo",
		},
		{
			`foo, "\}`,
			`foo\, \"\\\}`,
		},
	}

	for i, tt := range tests {
		actual, err := escapeAclItem(tt.input)

		if err != nil {
			t.Errorf("%d. Unexpected error %v", i, err)
		}

		if actual != tt.expected {
			t.Errorf("%d.\nexpected: %s,\nactual:   %s", i, tt.expected, actual)
		}
	}
}

func TestParseAclItemArray(t *testing.T) {
	tests := []struct {
		input    string
		expected []AclItem
		errMsg   string
	}{
		{
			"",
			[]AclItem{},
			"",
		},
		{
			"one",
			[]AclItem{"one"},
			"",
		},
		{
			`"one"`,
			[]AclItem{"one"},
			"",
		},
		{
			"one,two,three",
			[]AclItem{"one", "two", "three"},
			"",
		},
		{
			`"one","two","three"`,
			[]AclItem{"one", "two", "three"},
			"",
		},
		{
			`"one",two,"three"`,
			[]AclItem{"one", "two", "three"},
			"",
		},
		{
			`one,two,"three"`,
			[]AclItem{"one", "two", "three"},
			"",
		},
		{
			`"one","two",three`,
			[]AclItem{"one", "two", "three"},
			"",
		},
		{
			`"one","t w o",three`,
			[]AclItem{"one", "t w o", "three"},
			"",
		},
		{
			`"one","t, w o\"\}\\",three`,
			[]AclItem{"one", `t, w o"}\`, "three"},
			"",
		},
		{
			`"one","two",three"`,
			[]AclItem{"one", "two", `three"`},
			"",
		},
		{
			`"one","two,"three"`,
			nil,
			"unexpected rune after quoted value",
		},
		{
			`"one","two","three`,
			nil,
			"unexpected end of quoted value",
		},
	}

	for i, tt := range tests {
		actual, err := parseAclItemArray(tt.input)

		if err != nil {
			if tt.errMsg == "" {
				t.Errorf("%d. Unexpected error %v", i, err)
			} else if err.Error() != tt.errMsg {
				t.Errorf("%d. Expected error %v did not match actual error %v", i, tt.errMsg, err.Error())
			}
		} else if tt.errMsg != "" {
			t.Errorf("%d. Expected error not returned: \"%v\"", i, tt.errMsg)
		}

		if !reflect.DeepEqual(actual, tt.expected) {
			t.Errorf("%d. Expected %v did not match actual %v", i, tt.expected, actual)
		}
	}
}
