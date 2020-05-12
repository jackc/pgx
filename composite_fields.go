package pgtype

import (
	"encoding/binary"

	"github.com/jackc/pgio"
	errors "golang.org/x/xerrors"
)

// CompositeFields scans the fields of a composite type into the elements of the CompositeFields value. To scan a
// nullable value use a *CompositeFields. It will be set to nil in case of null.
//
// CompositeFields implements EncodeBinary and EncodeText. However, functionality is limited due to CompositeFields not
// knowing the PostgreSQL schema of the composite type. Prefer using a registered CompositeType.
type CompositeFields []interface{}

func (cf CompositeFields) DecodeBinary(ci *ConnInfo, src []byte) error {
	if len(cf) == 0 {
		return errors.Errorf("cannot decode into empty CompositeFields")
	}

	if src == nil {
		return errors.Errorf("cannot decode unexpected null into CompositeFields")
	}

	scanner, err := NewCompositeBinaryScanner(src)
	if err != nil {
		return err
	}
	if len(cf) != scanner.FieldCount() {
		return errors.Errorf("SQL composite can't be read, field count mismatch. expected %d , found %d", len(cf), scanner.FieldCount())
	}

	for i := 0; scanner.Scan(); i++ {
		err := ci.Scan(scanner.OID(), BinaryFormatCode, scanner.Bytes(), cf[i])
		if err != nil {
			return err
		}
	}

	if scanner.Err() != nil {
		return scanner.Err()
	}

	return nil
}

func (cf CompositeFields) DecodeText(ci *ConnInfo, src []byte) error {
	if len(cf) == 0 {
		return errors.Errorf("cannot decode into empty CompositeFields")
	}

	if src == nil {
		return errors.Errorf("cannot decode unexpected null into CompositeFields")
	}

	scanner, err := NewCompositeTextScanner(src)
	if err != nil {
		return err
	}

	fieldCount := 0

	for i := 0; scanner.Scan(); i++ {
		err := ci.Scan(0, TextFormatCode, scanner.Bytes(), cf[i])
		if err != nil {
			return err
		}

		fieldCount += 1
	}

	if scanner.Err() != nil {
		return scanner.Err()
	}

	if len(cf) != fieldCount {
		return errors.Errorf("SQL composite can't be read, field count mismatch. expected %d , found %d", len(cf), fieldCount)
	}

	return nil
}

// EncodeText encodes composite fields into the text format. Prefer registering a CompositeType to using
// CompositeFields to encode directly.
func (cf CompositeFields) EncodeText(ci *ConnInfo, buf []byte) ([]byte, error) {
	buf = append(buf, '(')

	fieldBuf := make([]byte, 0, 32)

	for _, f := range cf {
		if f != nil {
			fieldBuf = fieldBuf[0:0]
			if textEncoder, ok := f.(TextEncoder); ok {
				var err error
				fieldBuf, err = textEncoder.EncodeText(ci, fieldBuf)
				if err != nil {
					return nil, err
				}
				if fieldBuf != nil {
					buf = append(buf, QuoteCompositeFieldIfNeeded(string(fieldBuf))...)
				}
			} else {
				dt, ok := ci.DataTypeForValue(f)
				if !ok {
					return nil, errors.Errorf("Unknown data type for %#v", f)
				}

				err := dt.Value.Set(f)
				if err != nil {
					return nil, err
				}

				if textEncoder, ok := dt.Value.(TextEncoder); ok {
					var err error
					fieldBuf, err = textEncoder.EncodeText(ci, fieldBuf)
					if err != nil {
						return nil, err
					}
					if fieldBuf != nil {
						buf = append(buf, QuoteCompositeFieldIfNeeded(string(fieldBuf))...)
					}
				} else {
					return nil, errors.Errorf("Cannot encode text format for %v", f)
				}
			}
		}
		buf = append(buf, ',')
	}

	buf[len(buf)-1] = ')'
	return buf, nil
}

// EncodeBinary encodes composite fields into the binary format. Unlike CompositeType the schema of the destination is
// unknown. Prefer registering a CompositeType to using CompositeFields to encode directly. Because the binary
// composite format requires the OID of each field to be specified the only types that will work are those known to
// ConnInfo.
//
// In particular:
//
// * Nil cannot be used because there is no way to determine what type it.
// * Integer types must be exact matches. e.g. A Go int32 into a PostgreSQL bigint will fail.
// * No dereferencing will be done. e.g. *Text must be used instead of Text.
func (cf CompositeFields) EncodeBinary(ci *ConnInfo, buf []byte) ([]byte, error) {
	buf = pgio.AppendUint32(buf, uint32(len(cf)))

	for _, f := range cf {
		dt, ok := ci.DataTypeForValue(f)
		if !ok {
			return nil, errors.Errorf("Unknown OID for %#v", f)
		}

		buf = pgio.AppendUint32(buf, dt.OID)
		lengthPos := len(buf)
		buf = pgio.AppendInt32(buf, -1)

		if binaryEncoder, ok := f.(BinaryEncoder); ok {
			fieldBuf, err := binaryEncoder.EncodeBinary(ci, buf)
			if err != nil {
				return nil, err
			}
			if fieldBuf != nil {
				binary.BigEndian.PutUint32(buf[lengthPos:], uint32(len(fieldBuf)-len(buf)))
				buf = fieldBuf
			}
		} else {
			err := dt.Value.Set(f)
			if err != nil {
				return nil, err
			}
			if binaryEncoder, ok := dt.Value.(BinaryEncoder); ok {
				fieldBuf, err := binaryEncoder.EncodeBinary(ci, buf)
				if err != nil {
					return nil, err
				}
				if fieldBuf != nil {
					binary.BigEndian.PutUint32(buf[lengthPos:], uint32(len(fieldBuf)-len(buf)))
					buf = fieldBuf
				}
			} else {
				return nil, errors.Errorf("Cannot encode binary format for %v", f)
			}
		}
	}

	return buf, nil
}
