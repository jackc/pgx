package pgtype_test

import (
	"testing"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgtype/binary"
	errors "golang.org/x/xerrors"
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

func (dst *MyCompositeRaw) DecodeBinary(ci *pgtype.ConnInfo, src []byte) error {
	a := pgtype.Int4{}
	b := pgtype.Text{}

	fieldIter, fieldCount, err := binary.NewRecordFieldIterator(src)
	if err != nil {
		return err
	}

	if 2 != fieldCount {
		return errors.Errorf("can't scan row value, number of fields don't match: found=%d expected=2", fieldCount)
	}

	_, fieldBytes, eof, err := fieldIter.Next()
	if eof || err != nil {
		return errors.New("Bad record")
	}
	if err = a.DecodeBinary(ci, fieldBytes); err != nil {
		return err
	}

	_, fieldBytes, eof, err = fieldIter.Next()
	if eof || err != nil {
		return errors.New("Bad record")
	}
	if err = b.DecodeBinary(ci, fieldBytes); err != nil {
		return err
	}

	dst.a = a.Int
	if b.Status == pgtype.Present {
		dst.b = &b.String
	} else {
		dst.b = nil
	}

	return nil
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
	c := pgtype.NewComposite(&pgtype.Int4{}, &pgtype.Text{})

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		c.SetFields(f1, f2)
		buf, _ = c.EncodeBinary(ci, buf[:0])
	}
	x = buf
}

var dstRaw MyCompositeRaw

func BenchmarkBinaryDecodingManual(b *testing.B) {
	ci := pgtype.NewConnInfo()
	buf, _ := MyType{4, ptrS("ABCDEFG")}.EncodeBinary(ci, nil)
	dst := MyCompositeRaw{}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		err := dst.DecodeBinary(ci, buf)
		E(err)
	}
	dstRaw = dst
}

var dstMyType MyType

func BenchmarkBinaryDecodingHelpers(b *testing.B) {
	ci := pgtype.NewConnInfo()
	buf, _ := MyType{4, ptrS("ABCDEFG")}.EncodeBinary(ci, nil)
	dst := MyType{}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		err := dst.DecodeBinary(ci, buf)
		E(err)
	}
	dstMyType = dst
}

var gf1 int
var gf2 *string

func BenchmarkBinaryDecodingCompositeScan(b *testing.B) {
	ci := pgtype.NewConnInfo()
	buf, _ := MyType{4, ptrS("ABCDEFG")}.EncodeBinary(ci, nil)
	var isNull bool
	var f1 int
	var f2 *string

	c := pgtype.NewComposite(&pgtype.Int4{}, &pgtype.Text{})

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		err := c.Scan(&isNull, &f1, &f2).DecodeBinary(ci, buf)
		E(err)
	}
	gf1 = f1
	gf2 = f2
}
