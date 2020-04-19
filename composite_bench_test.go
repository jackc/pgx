package pgtype_test

import (
	"testing"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgtype/binary"
)

type MyCompositeRaw struct {
	a int32
	b *string
}

func (src MyCompositeRaw) EncodeBinary(ci *pgtype.ConnInfo, buf []byte) (newBuf []byte, err error) {
	a := pgtype.Int4{src.a, pgtype.Present}

	fieldBytes := make([]byte, 0, 64)
	fieldBytes, _ = a.EncodeBinary(ci, fieldBytes[:0])

	newBuf = binary.RecordStart(buf, 2)
	newBuf = binary.RecordAdd(newBuf, pgtype.Int4OID, fieldBytes)

	if src.b != nil {
		fieldBytes, _ = pgtype.Text{*src.b, pgtype.Present}.EncodeBinary(ci, fieldBytes[:0])
		newBuf = binary.RecordAdd(newBuf, pgtype.TextOID, fieldBytes)
	} else {
		newBuf = binary.RecordAddNull(newBuf, pgtype.TextOID)
	}
	return
}

var x []byte

func BenchmarkBinaryEncodingManual(b *testing.B) {
	buf := make([]byte, 0, 128)
	ci := pgtype.NewConnInfo()
	v := MyCompositeRaw{4, ptrS("ABCDEFG")}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		buf, _ = v.EncodeBinary(ci, buf[:0])
	}
	x = buf
}

func BenchmarkBinaryEncodingHelper(b *testing.B) {
	buf := make([]byte, 0, 128)
	ci := pgtype.NewConnInfo()
	v := MyType{4, ptrS("ABCDEFG")}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		buf, _ = v.EncodeBinary(ci, buf[:0])
	}
	x = buf
}

func BenchmarkBinaryEncodingRow(b *testing.B) {
	buf := make([]byte, 0, 128)
	ci := pgtype.NewConnInfo()
	f1 := 2
	f2 := ptrS("bar")

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		c := pgtype.Composite(&pgtype.Int4{}, &pgtype.Text{})
		c.Set(pgtype.Row(f1, f2))
		buf, _ = c.EncodeBinary(ci, buf[:0])
	}
	x = buf
}
