package sanitize_test

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/internal/sanitize"
)

func FuzzQuoteString(f *testing.F) {
	const prefix = "prefix"
	f.Add("new\nline")
	f.Add("sample text")
	f.Add("sample q'u'o't'e's")
	f.Add("select 'quoted $42', $1")

	f.Fuzz(func(t *testing.T, input string) {
		got := string(sanitize.QuoteString([]byte(prefix), input))
		want := oldQuoteString(input)

		quoted, ok := strings.CutPrefix(got, prefix)
		if !ok {
			t.Fatalf("result has no prefix")
		}

		if want != quoted {
			t.Errorf("got  %q", got)
			t.Fatalf("want %q", want)
		}
	})
}

func FuzzQuoteBytes(f *testing.F) {
	const prefix = "prefix"
	f.Add([]byte(nil))
	f.Add([]byte("\n"))
	f.Add([]byte("sample text"))
	f.Add([]byte("sample q'u'o't'e's"))
	f.Add([]byte("select 'quoted $42', $1"))

	f.Fuzz(func(t *testing.T, input []byte) {
		got := string(sanitize.QuoteBytes([]byte(prefix), input))
		want := oldQuoteBytes(input)

		quoted, ok := strings.CutPrefix(got, prefix)
		if !ok {
			t.Fatalf("result has no prefix")
		}

		if want != quoted {
			t.Errorf("got  %q", got)
			t.Fatalf("want %q", want)
		}
	})
}
