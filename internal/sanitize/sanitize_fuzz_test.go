package sanitize_test

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/internal/sanitize"
)

func FuzzNewQuery(f *testing.F) {
	// Basic placeholders
	f.Add("select $1")
	f.Add("select $1, $2, $3")
	f.Add("select $1 from t where a = $2 and b = $1")
	f.Add("$1")
	f.Add("$0")
	f.Add("$$")
	f.Add("$")
	f.Add("$abc")

	// Single-quoted strings with placeholders inside (should be ignored)
	f.Add("select 'quoted $42', $1")
	f.Add("select 'foo''bar', $1")
	f.Add("select '''', $1")
	f.Add("'unterminated")
	f.Add("'ends with backslash\\")

	// Double-quoted identifiers
	f.Add(`select "doubled quoted $42", $1`)
	f.Add(`select "foo""bar", $1`)
	f.Add(`select """", $1`)
	f.Add(`"unterminated`)

	// Escape strings (E'...')
	f.Add("select E'escape \\'string', $1")
	f.Add("select e'lower case escape \\' $1', $2")
	f.Add("E'unterminated")
	f.Add("E'backslash at end\\")
	f.Add("E''")
	f.Add("E'\\''")

	// Line comments
	f.Add("select $1 -- line comment")
	f.Add("select $1 -- comment with $2\nand $2")
	f.Add("--entire line is comment $1")
	f.Add("-- unterminated comment")
	f.Add("select $1--$2")

	// Block comments including nesting
	f.Add("select $1 /* block comment */")
	f.Add("select /* nested /* comment */ */ $1")
	f.Add("/* unterminated")
	f.Add("/* also /* nested /* unterminated */")
	f.Add("select $1 /* comment with $2 */ and $2")
	f.Add("/**/")

	// Edge cases: empty, whitespace, unicode
	f.Add("")
	f.Add("   ")
	f.Add("\n\r\t")
	f.Add("select '日本語', $1")
	f.Add("select $1 -- コメント")
	f.Add("\xc0\xc0")                 // invalid UTF-8
	f.Add("select \xff $1")           // invalid UTF-8 mid-query
	f.Add("select '\xef\xbf\xbd' $1") // U+FFFD replacement character (width 3)

	// SQL injection patterns the lexer must handle
	f.Add("select $1; drop table users--")
	f.Add("select $1 union select $2")
	f.Add("select $1 where '1'='1'")

	// Transitions between states
	f.Add("E'esc' 'norm' \"ident\" $1 -- comment\n$2 /* block */ $3")
	f.Add("$1'$2'$3\"$4\"$5--$6\n$7/*$8*/$9")

	f.Fuzz(func(t *testing.T, input string) {
		query, err := sanitize.NewQuery(input)
		if err != nil {
			return
		}

		// Count the number of placeholder args the query expects.
		maxArg := 0
		for _, part := range query.Parts {
			if n, ok := part.(int); ok && n > maxArg {
				maxArg = n
			}
		}

		// Skip sanitization for unreasonably large placeholder numbers to avoid OOM in the test itself.
		if maxArg > 65536 {
			return
		}

		// Build args list and sanitize. Should not panic.
		args := make([]any, maxArg)
		for i := range args {
			args[i] = int64(i + 1)
		}
		_, _ = query.Sanitize(args...)
	})
}

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
