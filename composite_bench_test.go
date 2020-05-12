package pgtype_test

import (
	"testing"

	"github.com/jackc/pgtype"
	errors "golang.org/x/xerrors"
)

type MyCompositeRaw struct {
	A int32
	B *string
}

func (src MyCompositeRaw) EncodeBinary(ci *pgtype.ConnInfo, buf []byte) (newBuf []byte, err error) {
	a := pgtype.Int4{src.A, pgtype.Present}

	fieldBytes := make([]byte, 0, 64)
	fieldBytes, _ = a.EncodeBinary(ci, fieldBytes[:0])

	newBuf = pgtype.RecordStart(buf, 2)
	newBuf = pgtype.RecordAdd(newBuf, pgtype.Int4OID, fieldBytes)

	if src.B != nil {
		fieldBytes, _ = pgtype.Text{*src.B, pgtype.Present}.EncodeBinary(ci, fieldBytes[:0])
		newBuf = pgtype.RecordAdd(newBuf, pgtype.TextOID, fieldBytes)
	} else {
		newBuf = pgtype.RecordAddNull(newBuf, pgtype.TextOID)
	}
	return
}

func (dst *MyCompositeRaw) DecodeBinary(ci *pgtype.ConnInfo, src []byte) error {
	a := pgtype.Int4{}
	b := pgtype.Text{}

	scanner, err := pgtype.NewCompositeBinaryScanner(src)
	if err != nil {
		return err
	}

	if 2 != scanner.FieldCount() {
		return errors.Errorf("can't scan row value, number of fields don't match: found=%d expected=2", scanner.FieldCount())
	}

	if scanner.Scan() {
		if err = a.DecodeBinary(ci, scanner.Bytes()); err != nil {
			return err
		}
	}

	if scanner.Scan() {
		if err = b.DecodeBinary(ci, scanner.Bytes()); err != nil {
			return err
		}
	}

	if scanner.Err() != nil {
		return scanner.Err()
	}

	dst.A = a.Int
	if b.Status == pgtype.Present {
		dst.B = &b.String
	} else {
		dst.B = nil
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

func BenchmarkBinaryEncodingComposite(b *testing.B) {
	buf := make([]byte, 0, 128)
	ci := pgtype.NewConnInfo()
	f1 := 2
	f2 := ptrS("bar")
	c := pgtype.NewCompositeType(&pgtype.Int4{}, &pgtype.Text{})

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		c.Set([]interface{}{f1, f2})
		buf, _ = c.EncodeBinary(ci, buf[:0])
	}
	x = buf
}

func BenchmarkBinaryEncodingJSON(b *testing.B) {
	buf := make([]byte, 0, 128)
	ci := pgtype.NewConnInfo()
	v := MyCompositeRaw{4, ptrS("ABCDEFG")}
	j := pgtype.JSON{}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		j.Set(v)
		buf, _ = j.EncodeBinary(ci, buf[:0])
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
	var f1 int
	var f2 *string

	c := pgtype.NewCompositeType(&pgtype.Int4{}, &pgtype.Text{})

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		err := c.DecodeBinary(ci, buf)
		if err != nil {
			b.Fatal(err)
		}
		err = c.AssignTo([]interface{}{&f1, &f2})
		if err != nil {
			b.Fatal(err)
		}
	}
	gf1 = f1
	gf2 = f2
}

func BenchmarkBinaryDecodingJSON(b *testing.B) {
	ci := pgtype.NewConnInfo()
	j := pgtype.JSON{}
	j.Set(MyCompositeRaw{4, ptrS("ABCDEFG")})
	buf, _ := j.EncodeBinary(ci, nil)

	j = pgtype.JSON{}
	dst := MyCompositeRaw{}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		err := j.DecodeBinary(ci, buf)
		E(err)
		err = j.AssignTo(&dst)
		E(err)
	}
	dstRaw = dst
}
