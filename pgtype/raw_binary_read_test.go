package pgtype_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// rawReadRe matches the byte-order accessors that read an integer out of a
// slice without any bounds check of their own.
var rawReadRe = regexp.MustCompile(`binary\.(BigEndian|LittleEndian)\.Uint(16|32|64)\(`)

// Decoders in this package parse input controlled by the server. Reading an
// integer straight out of the source slice is only safe if the length was
// checked first, and keeping the check and the read as separate statements is
// what produced a long run of index-out-of-range panics on malformed values.
//
// Every read goes through internal/pgio instead: pgio.Reader for values with
// internal structure, where the bounds check is part of each read and the first
// failure is sticky, or the pgio.Uint*Exact functions for a fixed-size value
// that makes up an entire message. Both make the checked form the short form.
//
// This test enforces that. It is the reason the pattern stays gone rather than
// coming back the next time a decoder is added.
func TestNoRawBinaryReadsInDecoders(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}

	var offenders []string
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || strings.HasSuffix(name, "_test.go") {
			continue
		}
		if filepath.Ext(name) != ".go" && !strings.HasSuffix(name, ".go.erb") {
			continue
		}

		src, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}

		for i, line := range strings.Split(string(src), "\n") {
			if rawReadRe.MatchString(line) {
				offenders = append(offenders, filepath.Join("pgtype", name)+":"+strconv.Itoa(i+1)+": "+strings.TrimSpace(line))
			}
		}
	}

	if len(offenders) > 0 {
		t.Errorf("binary decoders must read through internal/pgio, not encoding/binary directly.\n"+
			"Use pgio.Reader for a value with internal structure, or pgio.Uint16Exact/Uint32Exact/Uint64Exact\n"+
			"for a fixed-size value that is the whole message.\n\nFound:\n  %s",
			strings.Join(offenders, "\n  "))
	}
}
