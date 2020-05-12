package pgtype

import (
	"encoding/binary"
	"strings"

	"github.com/jackc/pgio"
	errors "golang.org/x/xerrors"
)

type CompositeType struct {
	status Status

	typeName string
	fields   []Value
}

// NewCompositeType creates a Composite object, which acts as a "schema" for
// SQL composite values.
// To pass Composite as SQL parameter first set it's fields, either by
// passing initialized Value{} instances to NewCompositeType or by calling
// SetFields method
// To read composite fields back pass result of Scan() method
// to query Scan function.
func NewCompositeType(typeName string, fields ...Value) *CompositeType {
	return &CompositeType{typeName: typeName, fields: fields}
}

func (src CompositeType) Get() interface{} {
	switch src.status {
	case Present:
		results := make([]interface{}, len(src.fields))
		for i := range results {
			results[i] = src.fields[i].Get()
		}
		return results
	case Null:
		return nil
	default:
		return src.status
	}
}

func (ct *CompositeType) NewTypeValue() Value {
	a := &CompositeType{
		typeName: ct.typeName,
		fields:   make([]Value, len(ct.fields)),
	}

	for i := range ct.fields {
		a.fields[i] = NewValue(ct.fields[i])
	}

	return a
}

func (ct *CompositeType) TypeName() string {
	return ct.typeName
}

func (dst *CompositeType) Set(src interface{}) error {
	if src == nil {
		dst.status = Null
		return nil
	}

	switch value := src.(type) {
	case []interface{}:
		if len(value) != len(dst.fields) {
			return errors.Errorf("Number of fields don't match. CompositeType has %d fields", len(dst.fields))
		}
		for i, v := range value {
			if err := dst.fields[i].Set(v); err != nil {
				return err
			}
		}
		dst.status = Present
	case *[]interface{}:
		if value == nil {
			dst.status = Null
			return nil
		}
		return dst.Set(*value)
	default:
		return errors.Errorf("Can not convert %v to Composite", src)
	}

	return nil
}

// AssignTo should never be called on composite value directly
func (src CompositeType) AssignTo(dst interface{}) error {
	switch src.status {
	case Present:
		switch v := dst.(type) {
		case []interface{}:
			if len(v) != len(src.fields) {
				return errors.Errorf("Number of fields don't match. CompositeType has %d fields", len(src.fields))
			}
			for i := range src.fields {
				if v[i] == nil {
					continue
				}

				assignToErr := src.fields[i].AssignTo(v[i])
				if assignToErr != nil {
					// Try to use get / set instead -- this avoids every type having to be able to AssignTo type of self.
					setSucceeded := false
					if setter, ok := v[i].(Value); ok {
						err := setter.Set(src.fields[i].Get())
						setSucceeded = err == nil
					}
					if !setSucceeded {
						return errors.Errorf("unable to assign to dst[%d]: %v", i, assignToErr)
					}
				}

			}
			return nil
		case *[]interface{}:
			return src.AssignTo(*v)
		default:
			if nextDst, retry := GetAssignToDstType(dst); retry {
				return src.AssignTo(nextDst)
			}
			return errors.Errorf("unable to assign to %T", dst)
		}
	case Null:
		return NullAssignTo(dst)
	}
	return errors.Errorf("cannot decode %#v into %T", src, dst)
}

func (src CompositeType) EncodeBinary(ci *ConnInfo, buf []byte) (newBuf []byte, err error) {
	switch src.status {
	case Null:
		return nil, nil
	case Undefined:
		return nil, errUndefined
	}
	return EncodeRow(ci, buf, src.fields...)
}

// DecodeBinary implements BinaryDecoder interface.
// Opposite to Record, fields in a composite act as a "schema"
// and decoding fails if SQL value can't be assigned due to
// type mismatch
func (dst *CompositeType) DecodeBinary(ci *ConnInfo, buf []byte) (err error) {
	if buf == nil {
		dst.status = Null
		return nil
	}

	scanner, err := NewCompositeBinaryScanner(buf)
	if err != nil {
		return err
	}
	if len(dst.fields) != scanner.FieldCount() {
		return errors.Errorf("SQL composite can't be read, field count mismatch. expected %d , found %d", len(dst.fields), scanner.FieldCount())
	}

	for i := 0; scanner.Scan(); i++ {
		binaryDecoder, ok := dst.fields[i].(BinaryDecoder)
		if !ok {
			return errors.New("Composite field doesn't support binary protocol")
		}

		if err = binaryDecoder.DecodeBinary(ci, scanner.Bytes()); err != nil {
			return err
		}
	}

	if scanner.Err() != nil {
		return scanner.Err()
	}

	dst.status = Present

	return nil
}

type CompositeBinaryScanner struct {
	rp  int
	src []byte

	fieldCount int32
	fieldBytes []byte
	fieldOID   uint32
	err        error
}

// NewCompositeBinaryScanner a scanner over a binary encoded composite balue.
func NewCompositeBinaryScanner(src []byte) (CompositeBinaryScanner, error) {
	rp := 0
	if len(src[rp:]) < 4 {
		return CompositeBinaryScanner{}, errors.Errorf("Record incomplete %v", src)
	}

	fieldCount := int32(binary.BigEndian.Uint32(src[rp:]))
	rp += 4

	return CompositeBinaryScanner{
		rp:         rp,
		src:        src,
		fieldCount: fieldCount,
	}, nil
}

// Scan advances the scanner to the next field. It returns false after the last field is read or an error occurs. After
// Scan returns false, the Err method can be called to check if any errors occurred.
func (cfs *CompositeBinaryScanner) Scan() bool {
	if cfs.err != nil {
		return false
	}

	if cfs.rp == len(cfs.src) {
		return false
	}

	if len(cfs.src[cfs.rp:]) < 8 {
		cfs.err = errors.Errorf("Record incomplete %v", cfs.src)
		return false
	}
	cfs.fieldOID = binary.BigEndian.Uint32(cfs.src[cfs.rp:])
	cfs.rp += 4

	fieldLen := int(int32(binary.BigEndian.Uint32(cfs.src[cfs.rp:])))
	cfs.rp += 4

	if fieldLen >= 0 {
		if len(cfs.src[cfs.rp:]) < fieldLen {
			cfs.err = errors.Errorf("Record incomplete rp=%d src=%v", cfs.rp, cfs.src)
			return false
		}
		cfs.fieldBytes = cfs.src[cfs.rp : cfs.rp+fieldLen]
		cfs.rp += fieldLen
	} else {
		cfs.fieldBytes = nil
	}

	return true
}

func (cfs *CompositeBinaryScanner) FieldCount() int {
	return int(cfs.fieldCount)
}

// Bytes returns the bytes of the field most recently read by Scan().
func (cfs *CompositeBinaryScanner) Bytes() []byte {
	return cfs.fieldBytes
}

// OID returns the OID of the field most recently read by Scan().
func (cfs *CompositeBinaryScanner) OID() uint32 {
	return cfs.fieldOID
}

// Err returns any error encountered by the scanner.
func (cfs *CompositeBinaryScanner) Err() error {
	return cfs.err
}

type CompositeTextScanner struct {
	rp  int
	src []byte

	fieldBytes []byte
	err        error
}

// NewCompositeTextScanner a scanner over a text encoded composite balue.
func NewCompositeTextScanner(src []byte) (CompositeTextScanner, error) {
	if len(src) < 2 {
		return CompositeTextScanner{}, errors.Errorf("Record incomplete %v", src)
	}

	if src[0] != '(' {
		return CompositeTextScanner{}, errors.Errorf("composite text format must start with '('")
	}

	if src[len(src)-1] != ')' {
		return CompositeTextScanner{}, errors.Errorf("composite text format must end with ')'")
	}

	return CompositeTextScanner{
		rp:  1,
		src: src,
	}, nil
}

// Scan advances the scanner to the next field. It returns false after the last field is read or an error occurs. After
// Scan returns false, the Err method can be called to check if any errors occurred.
func (cfs *CompositeTextScanner) Scan() bool {
	if cfs.err != nil {
		return false
	}

	if cfs.rp == len(cfs.src) {
		return false
	}

	switch cfs.src[cfs.rp] {
	case ',', ')': // null
		cfs.rp++
		cfs.fieldBytes = nil
		return true
	case '"': // quoted value
		cfs.rp++
		cfs.fieldBytes = make([]byte, 0, 16)
		for {
			ch := cfs.src[cfs.rp]

			if ch == '"' {
				cfs.rp++
				if cfs.src[cfs.rp] == '"' {
					cfs.fieldBytes = append(cfs.fieldBytes, '"')
					cfs.rp++
				} else {
					break
				}
			} else {
				cfs.fieldBytes = append(cfs.fieldBytes, ch)
				cfs.rp++
			}
		}
		cfs.rp++
		return true
	default: // unquoted value
		start := cfs.rp
		for {
			ch := cfs.src[cfs.rp]
			if ch == ',' || ch == ')' {
				break
			}
			cfs.rp++
		}
		cfs.fieldBytes = cfs.src[start:cfs.rp]
		cfs.rp++
		return true
	}
}

// Bytes returns the bytes of the field most recently read by Scan().
func (cfs *CompositeTextScanner) Bytes() []byte {
	return cfs.fieldBytes
}

// Err returns any error encountered by the scanner.
func (cfs *CompositeTextScanner) Err() error {
	return cfs.err
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

var quoteCompositeReplacer = strings.NewReplacer(`\`, `\\`, `"`, `\"`)

func quoteCompositeField(src string) string {
	return `"` + quoteCompositeReplacer.Replace(src) + `"`
}

func QuoteCompositeFieldIfNeeded(src string) string {
	if src == "" || src[0] == ' ' || src[len(src)-1] == ' ' || strings.ContainsAny(src, `(),"\`) {
		return quoteCompositeField(src)
	}
	return src
}
