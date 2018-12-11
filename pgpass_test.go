package pgx

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/user"
	"strings"
	"testing"
)

func unescape(s string) string {
	s = strings.Replace(s, `\:`, `:`, -1)
	s = strings.Replace(s, `\\`, `\`, -1)
	return s
}

var passfile = [][]string{
	{"test1", "5432", "larrydb", "larry", "whatstheidea"},
	{"test1", "5432", "moedb", "moe", "imbecile"},
	{"test1", "5432", "curlydb", "curly", "nyuknyuknyuk"},
	{"test2", "5432", "*", "shemp", "heymoe"},
	{"test2", "5432", "*", "*", `test\\ing\:`},
	{"localhost", "*", "*", "*", "sesam"},
	{"test3", "*", "", "", "swordfish"}, // user will be filled later
}

func TestPGPass(t *testing.T) {
	tf, err := ioutil.TempFile("", "")
	if err != nil {
		t.Fatal(err)
	}
	user, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	passfile[len(passfile)-1][2] = user.Username
	passfile[len(passfile)-1][3] = user.Username

	defer tf.Close()
	defer os.Remove(tf.Name())
	os.Setenv("PGPASSFILE", tf.Name())
	_, err = fmt.Fprintln(tf, "#some comment\n\n#more comment")
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range passfile {
		_, err := fmt.Fprintln(tf, strings.Join(l, `:`))
		if err != nil {
			t.Fatal(err)
		}
	}
	if err = tf.Close(); err != nil {
		t.Fatal(err)
	}
	for i, l := range passfile {
		cfg := ConnConfig{Host: l[0], Database: l[2], User: l[3]}
		found := pgpass(&cfg)
		if !found {
			t.Fatalf("Entry %v not found", i)
		}
		if cfg.Password != unescape(l[4]) {
			t.Fatalf(`Password mismatch entry %v want %s got %s`, i, unescape(l[4]), cfg.Password)
		}
		if l[0] == "localhost" {
			// using some existing path as socket
			cfg := ConnConfig{Host: tf.Name(), Database: l[2], User: l[3]}
			found := pgpass(&cfg)
			if !found {
				t.Fatalf("Entry %v not found", i)
			}
			if cfg.Password != unescape(l[4]) {
				t.Fatalf(`Password mismatch entry %v want %s got %s`, i, unescape(l[4]), cfg.Password)
			}
		}
	}
	cfg := ConnConfig{Host: "test3"}
	found := pgpass(&cfg)
	if !found {
		t.Fatalf("Entry for default user name")
	}
	if cfg.Password != "swordfish" {
		t.Fatalf(`Password mismatch for default user entry, want %s got %s`, "swordfish", cfg.Password)
	}
	cfg = ConnConfig{Host: "derp", Database: "herp", User: "joe"}
	found = pgpass(&cfg)
	if found {
		t.Fatal("bad found")
	}
}
