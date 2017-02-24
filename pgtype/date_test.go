package pgtype_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgio"
	"github.com/jackc/pgx/pgtype"
)

func TestDateTranscode(t *testing.T) {
	conn := mustConnectPgx(t)
	defer mustClose(t, conn)

	ps, err := conn.Prepare("test", "select $1::date")
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		result *pgtype.Date
	}{
		{result: pgtype.NewDate(1900, 1, 1)},
		{result: pgtype.NewDate(1970, 1, 1)},
		{result: pgtype.NewDate(1999, 12, 31)},
		{result: pgtype.NewDate(2000, 1, 1)},
		{result: pgtype.NewDate(2000, 1, 2)},
		{result: pgtype.NewDate(2200, 1, 1)},
	}

	ps.FieldDescriptions[0].FormatCode = pgx.TextFormatCode
	for i, tt := range tests {
		inputBuf := &bytes.Buffer{}
		err = tt.result.EncodeText(inputBuf)
		if err != nil {
			t.Errorf("TextFormat %d: %v", i, err)
		}

		var s string
		err := conn.QueryRow("test", string(inputBuf.Bytes()[4:])).Scan(&s)
		if err != nil {
			t.Errorf("TextFormat %d: %v", i, err)
		}

		outputBuf := &bytes.Buffer{}
		pgio.WriteInt32(outputBuf, int32(len(s)))
		outputBuf.WriteString(s)
		var r pgtype.Date
		err = r.DecodeText(outputBuf)
		if err != nil {
			t.Errorf("TextFormat %d: %v", i, err)
		}

		if r != *tt.result {
			t.Errorf("TextFormat %d: expected %v, got %v", i, *tt.result, r)
		}
	}

	ps.FieldDescriptions[0].FormatCode = pgx.BinaryFormatCode
	for i, tt := range tests {
		inputBuf := &bytes.Buffer{}
		err = tt.result.EncodeBinary(inputBuf)
		if err != nil {
			t.Errorf("BinaryFormat %d: %v", i, err)
		}

		var buf []byte
		err := conn.QueryRow("test", inputBuf.Bytes()[4:]).Scan(&buf)
		if err != nil {
			t.Errorf("BinaryFormat %d: %v", i, err)
		}

		outputBuf := &bytes.Buffer{}
		pgio.WriteInt32(outputBuf, int32(len(buf)))
		outputBuf.Write(buf)
		var r pgtype.Date
		err = r.DecodeBinary(outputBuf)
		if err != nil {
			t.Errorf("BinaryFormat %d: %v", i, err)
		}

		if r != *tt.result {
			t.Errorf("BinaryFormat %d: expected %v, got %v", i, tt.result, r)
		}
	}
}

func TestDateConvertFrom(t *testing.T) {
	type _time time.Time

	successfulTests := []struct {
		source interface{}
		result *pgtype.Date
	}{
		{source: time.Date(1900, 1, 1, 0, 0, 0, 0, time.Local), result: pgtype.NewDate(1900, 1, 1)},
		{source: time.Date(1970, 1, 1, 0, 0, 0, 0, time.Local), result: pgtype.NewDate(1970, 1, 1)},
		{source: time.Date(1970, 1, 1, 0, 0, 0, 0, time.Local), result: pgtype.NewDate(1999, 12, 31)},
		{source: time.Date(1970, 1, 1, 0, 0, 0, 0, time.Local), result: pgtype.NewDate(2000, 1, 1)},
		{source: time.Date(1970, 1, 1, 0, 0, 0, 0, time.Local), result: pgtype.NewDate(2000, 1, 2)},
		{source: time.Date(1970, 1, 1, 0, 0, 0, 0, time.Local), result: pgtype.NewDate(2200, 1, 1)},
		{source: _time(time.Date(1970, 1, 1, 0, 0, 0, 0, time.Local)), result: pgtype.NewDate(1970, 1, 1)},
	}

	for i, tt := range successfulTests {
		var d pgtype.Date
		err := d.ConvertFrom(tt.source)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}
	}
}
