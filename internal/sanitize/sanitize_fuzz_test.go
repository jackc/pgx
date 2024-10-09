package sanitize_test

import (
	"testing"

	"github.com/jackc/pgx/v5/internal/sanitize"
)

func FuzzQuoteString(f *testing.F) {
	f.Add("")
	f.Add("\n")
	f.Add("sample text")
	f.Add("sample q'u'o't'e's")
	f.Add("select 'quoted $42', $1")

	f.Fuzz(func(t *testing.T, input string) {
		got := sanitize.QuoteString(nil, input)
		want := oldQuoteString(input)

		if want != string(got) {
			t.Errorf("got  %q", got)
			t.Fatalf("want %q", want)
		}
	})
}

func FuzzQuoteBytes(f *testing.F) {
	f.Add([]byte(nil))
	f.Add([]byte("\n"))
	f.Add([]byte("sample text"))
	f.Add([]byte("sample q'u'o't'e's"))
	f.Add([]byte("select 'quoted $42', $1"))

	f.Fuzz(func(t *testing.T, input []byte) {
		got := sanitize.QuoteBytes(nil, input)
		want := oldQuoteBytes(input)

		if want != string(got) {
			t.Errorf("got  %q", got)
			t.Fatalf("want %q", want)
		}
	})
}
