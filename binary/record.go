package binary

import (
	"encoding/binary"

	"github.com/jackc/pgio"
	errors "golang.org/x/xerrors"
)

type RecordFieldIter struct {
	rp  int
	src []byte
}

// NewRecordFieldIterator creates iterator over binary representation
// of record, aka ROW(), aka Composite
func NewRecordFieldIterator(src []byte) (RecordFieldIter, int, error) {
	rp := 0
	if len(src[rp:]) < 4 {
		return RecordFieldIter{}, 0, errors.Errorf("Record incomplete %v", src)
	}

	fieldCount := int(int32(binary.BigEndian.Uint32(src[rp:])))
	rp += 4

	return RecordFieldIter{
		rp:  rp,
		src: src,
	}, fieldCount, nil
}

// Next returns next field decoded from record. eof is returned if no
// more fields left to decode.
func (fi *RecordFieldIter) Next() (fieldOID uint32, buf []byte, eof bool, err error) {
	if fi.rp == len(fi.src) {
		eof = true
		return
	}

	if len(fi.src[fi.rp:]) < 8 {
		err = errors.Errorf("Record incomplete %v", fi.src)
		return
	}
	fieldOID = binary.BigEndian.Uint32(fi.src[fi.rp:])
	fi.rp += 4

	fieldLen := int(int32(binary.BigEndian.Uint32(fi.src[fi.rp:])))
	fi.rp += 4

	if fieldLen >= 0 {
		if len(fi.src[fi.rp:]) < fieldLen {
			err = errors.Errorf("Record incomplete rp=%d src=%v", fi.rp, fi.src)
			return
		}
		buf = fi.src[fi.rp : fi.rp+fieldLen]
		fi.rp += fieldLen
	}

	return
}

// RecordStart adds record header to the buf
func RecordStart(buf []byte, fieldCount int) []byte {
	return pgio.AppendUint32(buf, uint32(fieldCount))
}

// RecordAdd adds record field to the buf
func RecordAdd(buf []byte, oid uint32, fieldBytes []byte) []byte {
	buf = pgio.AppendUint32(buf, oid)
	buf = pgio.AppendUint32(buf, uint32(len(fieldBytes)))
	buf = append(buf, fieldBytes...)
	return buf
}

// RecordAddNull adds null value as a field to the buf
func RecordAddNull(buf []byte, oid uint32) []byte {
	return pgio.AppendInt32(buf, int32(-1))
}
