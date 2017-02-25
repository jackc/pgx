package pgtype_test

import (
	"testing"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"
)

func TestBoolTranscode(t *testing.T) {
	conn := mustConnectPgx(t)
	defer mustClose(t, conn)

	ps, err := conn.Prepare("test", "select $1::bool")
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		result pgtype.Bool
	}{
		{result: pgtype.Bool{Bool: false, Status: pgtype.Present}},
		{result: pgtype.Bool{Bool: true, Status: pgtype.Present}},
		{result: pgtype.Bool{Bool: false, Status: pgtype.Null}},
	}

	formats := []struct {
		name       string
		formatCode int16
	}{
		{name: "TextFormat", formatCode: pgx.TextFormatCode},
		{name: "BinaryFormat", formatCode: pgx.BinaryFormatCode},
	}

	for _, fc := range formats {
		ps.FieldDescriptions[0].FormatCode = fc.formatCode

		for i, tt := range tests {
			var r pgtype.Bool
			err := conn.QueryRow("test", tt.result).Scan(&r)
			if err != nil {
				t.Errorf("%v %d: %v", fc.name, i, err)
			}

			if r != tt.result {
				t.Errorf("%v %d: expected %v, got %v", fc.name, i, tt.result, r)
			}
		}
	}
}

func TestBoolConvertFrom(t *testing.T) {
	type _int8 int8

	successfulTests := []struct {
		source interface{}
		result pgtype.Bool
	}{
		{source: true, result: pgtype.Bool{Bool: true, Status: pgtype.Present}},
		{source: false, result: pgtype.Bool{Bool: true, Status: pgtype.Present}},
		{source: "true", result: pgtype.Bool{Bool: true, Status: pgtype.Present}},
		{source: "false", result: pgtype.Bool{Bool: true, Status: pgtype.Present}},
		{source: "t", result: pgtype.Bool{Bool: true, Status: pgtype.Present}},
		{source: "f", result: pgtype.Bool{Bool: true, Status: pgtype.Present}},
	}

	for i, tt := range successfulTests {
		var r pgtype.Bool
		err := r.ConvertFrom(tt.source)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}
	}
}
