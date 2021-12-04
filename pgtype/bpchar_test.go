package pgtype_test

import (
	"reflect"
	"testing"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgtype/testutil"
)

func TestChar3Transcode(t *testing.T) {
	testutil.TestSuccessfulTranscodeEqFunc(t, "char(3)", []interface{}{
		&pgtype.BPChar{String: "a  ", Valid: true},
		&pgtype.BPChar{String: " a ", Valid: true},
		&pgtype.BPChar{String: "嗨  ", Valid: true},
		&pgtype.BPChar{String: "   ", Valid: true},
		&pgtype.BPChar{},
	}, func(aa, bb interface{}) bool {
		a := aa.(pgtype.BPChar)
		b := bb.(pgtype.BPChar)

		return a.Valid == b.Valid && a.String == b.String
	})
}

func TestBPCharAssignTo(t *testing.T) {
	var (
		str string
		run rune
	)
	simpleTests := []struct {
		src      pgtype.BPChar
		dst      interface{}
		expected interface{}
	}{
		{src: pgtype.BPChar{String: "simple", Valid: true}, dst: &str, expected: "simple"},
		{src: pgtype.BPChar{String: "嗨", Valid: true}, dst: &run, expected: '嗨'},
	}

	for i, tt := range simpleTests {
		err := tt.src.AssignTo(tt.dst)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if dst := reflect.ValueOf(tt.dst).Elem().Interface(); dst != tt.expected {
			t.Errorf("%d: expected %v to assign %v, but result was %v", i, tt.src, tt.expected, dst)
		}
	}

}
