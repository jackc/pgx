package pgtype

import (
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgio"
)

// CompositeIndexGetter is a type accessed by index that can be converted into a PostgreSQL composite.
type CompositeIndexGetter interface {
	// IsNull returns true if the value is SQL NULL.
	IsNull() bool

	// Index returns the element at i.
	Index(i int) interface{}
}

// CompositeIndexScanner is a type accessed by index that can be scanned from a PostgreSQL composite.
type CompositeIndexScanner interface {
	// ScanNull sets the value to SQL NULL.
	ScanNull() error

	// ScanIndex returns a value usable as a scan target for i.
	ScanIndex(i int) interface{}
}

type CompositeCodecField struct {
	Name     string
	DataType *DataType
}

type CompositeCodec struct {
	Fields []CompositeCodecField
}

func (c *CompositeCodec) FormatSupported(format int16) bool {
	for _, f := range c.Fields {
		if !f.DataType.Codec.FormatSupported(format) {
			return false
		}
	}

	return true
}

func (c *CompositeCodec) PreferredFormat() int16 {
	if c.FormatSupported(BinaryFormatCode) {
		return BinaryFormatCode
	}
	return TextFormatCode
}

func (c *CompositeCodec) PlanEncode(ci *ConnInfo, oid uint32, format int16, value interface{}) EncodePlan {
	if _, ok := value.(CompositeIndexGetter); !ok {
		return nil
	}

	switch format {
	case BinaryFormatCode:
		return &encodePlanCompositeCodecCompositeIndexGetterToBinary{cc: c, ci: ci}
	case TextFormatCode:
		return &encodePlanCompositeCodecCompositeIndexGetterToText{cc: c, ci: ci}
	}

	return nil
}

type encodePlanCompositeCodecCompositeIndexGetterToBinary struct {
	cc *CompositeCodec
	ci *ConnInfo
}

func (plan *encodePlanCompositeCodecCompositeIndexGetterToBinary) Encode(value interface{}, buf []byte) (newBuf []byte, err error) {
	getter := value.(CompositeIndexGetter)

	if getter.IsNull() {
		return nil, nil
	}

	builder := NewCompositeBinaryBuilder(plan.ci, buf)
	for i, field := range plan.cc.Fields {
		builder.AppendValue(field.DataType.OID, getter.Index(i))
	}

	return builder.Finish()
}

type encodePlanCompositeCodecCompositeIndexGetterToText struct {
	cc *CompositeCodec
	ci *ConnInfo
}

func (plan *encodePlanCompositeCodecCompositeIndexGetterToText) Encode(value interface{}, buf []byte) (newBuf []byte, err error) {
	getter := value.(CompositeIndexGetter)

	if getter.IsNull() {
		return nil, nil
	}

	b := NewCompositeTextBuilder(plan.ci, buf)
	for i, field := range plan.cc.Fields {
		b.AppendValue(field.DataType.OID, getter.Index(i))
	}

	return b.Finish()
}

func (c *CompositeCodec) PlanScan(ci *ConnInfo, oid uint32, format int16, target interface{}, actualTarget bool) ScanPlan {
	switch format {
	case BinaryFormatCode:
		switch target.(type) {
		case CompositeIndexScanner:
			return &scanPlanBinaryCompositeToCompositeIndexScanner{cc: c, ci: ci}
		}
	case TextFormatCode:
		switch target.(type) {
		case CompositeIndexScanner:
			return &scanPlanTextCompositeToCompositeIndexScanner{cc: c, ci: ci}
		}
	}

	return nil
}

type scanPlanBinaryCompositeToCompositeIndexScanner struct {
	cc *CompositeCodec
	ci *ConnInfo
}

func (plan *scanPlanBinaryCompositeToCompositeIndexScanner) Scan(src []byte, target interface{}) error {
	targetScanner := (target).(CompositeIndexScanner)

	if src == nil {
		return targetScanner.ScanNull()
	}

	scanner := NewCompositeBinaryScanner(plan.ci, src)
	for i, field := range plan.cc.Fields {
		if scanner.Next() {
			fieldTarget := targetScanner.ScanIndex(i)
			if fieldTarget != nil {
				fieldPlan := plan.ci.PlanScan(field.DataType.OID, BinaryFormatCode, fieldTarget)
				if fieldPlan == nil {
					return fmt.Errorf("unable to encode %v into OID %d in binary format", field, field.DataType.OID)
				}

				err := fieldPlan.Scan(scanner.Bytes(), fieldTarget)
				if err != nil {
					return err
				}
			}
		} else {
			return errors.New("read past end of composite")
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	return nil
}

type scanPlanTextCompositeToCompositeIndexScanner struct {
	cc *CompositeCodec
	ci *ConnInfo
}

func (plan *scanPlanTextCompositeToCompositeIndexScanner) Scan(src []byte, target interface{}) error {
	targetScanner := (target).(CompositeIndexScanner)

	if src == nil {
		return targetScanner.ScanNull()
	}

	scanner := NewCompositeTextScanner(plan.ci, src)
	for i, field := range plan.cc.Fields {
		if scanner.Next() {
			fieldTarget := targetScanner.ScanIndex(i)
			if fieldTarget != nil {
				fieldPlan := plan.ci.PlanScan(field.DataType.OID, TextFormatCode, fieldTarget)
				if fieldPlan == nil {
					return fmt.Errorf("unable to encode %v into OID %d in text format", field, field.DataType.OID)
				}

				err := fieldPlan.Scan(scanner.Bytes(), fieldTarget)
				if err != nil {
					return err
				}
			}
		} else {
			return errors.New("read past end of composite")
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	return nil
}

func (c *CompositeCodec) DecodeDatabaseSQLValue(ci *ConnInfo, oid uint32, format int16, src []byte) (driver.Value, error) {
	if src == nil {
		return nil, nil
	}

	switch format {
	case TextFormatCode:
		return string(src), nil
	case BinaryFormatCode:
		buf := make([]byte, len(src))
		copy(buf, src)
		return buf, nil
	default:
		return nil, fmt.Errorf("unknown format code %d", format)
	}
}

func (c *CompositeCodec) DecodeValue(ci *ConnInfo, oid uint32, format int16, src []byte) (interface{}, error) {
	if src == nil {
		return nil, nil
	}

	switch format {
	case TextFormatCode:
		scanner := NewCompositeTextScanner(ci, src)
		values := make(map[string]interface{}, len(c.Fields))
		for i := 0; scanner.Next() && i < len(c.Fields); i++ {
			var v interface{}
			fieldPlan := ci.PlanScan(c.Fields[i].DataType.OID, TextFormatCode, &v)
			if fieldPlan == nil {
				return nil, fmt.Errorf("unable to scan OID %d in text format into %v", c.Fields[i].DataType.OID, v)
			}

			err := fieldPlan.Scan(scanner.Bytes(), &v)
			if err != nil {
				return nil, err
			}

			values[c.Fields[i].Name] = v
		}

		if err := scanner.Err(); err != nil {
			return nil, err
		}

		return values, nil
	case BinaryFormatCode:
		scanner := NewCompositeBinaryScanner(ci, src)
		values := make(map[string]interface{}, len(c.Fields))
		for i := 0; scanner.Next() && i < len(c.Fields); i++ {
			var v interface{}
			fieldPlan := ci.PlanScan(scanner.OID(), BinaryFormatCode, &v)
			if fieldPlan == nil {
				return nil, fmt.Errorf("unable to scan OID %d in binary format into %v", scanner.OID(), v)
			}

			err := fieldPlan.Scan(scanner.Bytes(), &v)
			if err != nil {
				return nil, err
			}

			values[c.Fields[i].Name] = v
		}

		if err := scanner.Err(); err != nil {
			return nil, err
		}

		return values, nil
	default:
		return nil, fmt.Errorf("unknown format code %d", format)
	}

}

type CompositeBinaryScanner struct {
	ci  *ConnInfo
	rp  int
	src []byte

	fieldCount int32
	fieldBytes []byte
	fieldOID   uint32
	err        error
}

// NewCompositeBinaryScanner a scanner over a binary encoded composite balue.
func NewCompositeBinaryScanner(ci *ConnInfo, src []byte) *CompositeBinaryScanner {
	rp := 0
	if len(src[rp:]) < 4 {
		return &CompositeBinaryScanner{err: fmt.Errorf("Record incomplete %v", src)}
	}

	fieldCount := int32(binary.BigEndian.Uint32(src[rp:]))
	rp += 4

	return &CompositeBinaryScanner{
		ci:         ci,
		rp:         rp,
		src:        src,
		fieldCount: fieldCount,
	}
}

// Next advances the scanner to the next field. It returns false after the last field is read or an error occurs. After
// Next returns false, the Err method can be called to check if any errors occurred.
func (cfs *CompositeBinaryScanner) Next() bool {
	if cfs.err != nil {
		return false
	}

	if cfs.rp == len(cfs.src) {
		return false
	}

	if len(cfs.src[cfs.rp:]) < 8 {
		cfs.err = fmt.Errorf("Record incomplete %v", cfs.src)
		return false
	}
	cfs.fieldOID = binary.BigEndian.Uint32(cfs.src[cfs.rp:])
	cfs.rp += 4

	fieldLen := int(int32(binary.BigEndian.Uint32(cfs.src[cfs.rp:])))
	cfs.rp += 4

	if fieldLen >= 0 {
		if len(cfs.src[cfs.rp:]) < fieldLen {
			cfs.err = fmt.Errorf("Record incomplete rp=%d src=%v", cfs.rp, cfs.src)
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
	ci  *ConnInfo
	rp  int
	src []byte

	fieldBytes []byte
	err        error
}

// NewCompositeTextScanner a scanner over a text encoded composite value.
func NewCompositeTextScanner(ci *ConnInfo, src []byte) *CompositeTextScanner {
	if len(src) < 2 {
		return &CompositeTextScanner{err: fmt.Errorf("Record incomplete %v", src)}
	}

	if src[0] != '(' {
		return &CompositeTextScanner{err: fmt.Errorf("composite text format must start with '('")}
	}

	if src[len(src)-1] != ')' {
		return &CompositeTextScanner{err: fmt.Errorf("composite text format must end with ')'")}
	}

	return &CompositeTextScanner{
		ci:  ci,
		rp:  1,
		src: src,
	}
}

// Next advances the scanner to the next field. It returns false after the last field is read or an error occurs. After
// Next returns false, the Err method can be called to check if any errors occurred.
func (cfs *CompositeTextScanner) Next() bool {
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
			} else if ch == '\\' {
				cfs.rp++
				cfs.fieldBytes = append(cfs.fieldBytes, cfs.src[cfs.rp])
				cfs.rp++
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

type CompositeBinaryBuilder struct {
	ci         *ConnInfo
	buf        []byte
	startIdx   int
	fieldCount uint32
	err        error
}

func NewCompositeBinaryBuilder(ci *ConnInfo, buf []byte) *CompositeBinaryBuilder {
	startIdx := len(buf)
	buf = append(buf, 0, 0, 0, 0) // allocate room for number of fields
	return &CompositeBinaryBuilder{ci: ci, buf: buf, startIdx: startIdx}
}

func (b *CompositeBinaryBuilder) AppendValue(oid uint32, field interface{}) {
	if b.err != nil {
		return
	}

	if field == nil {
		b.buf = pgio.AppendUint32(b.buf, oid)
		b.buf = pgio.AppendInt32(b.buf, -1)
		b.fieldCount++
		return
	}

	plan := b.ci.PlanEncode(oid, BinaryFormatCode, field)
	if plan == nil {
		b.err = fmt.Errorf("unable to encode %v into OID %d in binary format", field, oid)
		return
	}

	b.buf = pgio.AppendUint32(b.buf, oid)
	lengthPos := len(b.buf)
	b.buf = pgio.AppendInt32(b.buf, -1)
	fieldBuf, err := plan.Encode(field, b.buf)
	if err != nil {
		b.err = err
		return
	}
	if fieldBuf != nil {
		binary.BigEndian.PutUint32(fieldBuf[lengthPos:], uint32(len(fieldBuf)-len(b.buf)))
		b.buf = fieldBuf
	}

	b.fieldCount++
}

func (b *CompositeBinaryBuilder) Finish() ([]byte, error) {
	if b.err != nil {
		return nil, b.err
	}

	binary.BigEndian.PutUint32(b.buf[b.startIdx:], b.fieldCount)
	return b.buf, nil
}

type CompositeTextBuilder struct {
	ci         *ConnInfo
	buf        []byte
	startIdx   int
	fieldCount uint32
	err        error
	fieldBuf   [32]byte
}

func NewCompositeTextBuilder(ci *ConnInfo, buf []byte) *CompositeTextBuilder {
	buf = append(buf, '(') // allocate room for number of fields
	return &CompositeTextBuilder{ci: ci, buf: buf}
}

func (b *CompositeTextBuilder) AppendValue(oid uint32, field interface{}) {
	if b.err != nil {
		return
	}

	if field == nil {
		b.buf = append(b.buf, ',')
		return
	}

	plan := b.ci.PlanEncode(oid, TextFormatCode, field)
	if plan == nil {
		b.err = fmt.Errorf("unable to encode %v into OID %d in text format", field, oid)
		return
	}

	fieldBuf, err := plan.Encode(field, b.fieldBuf[0:0])
	if err != nil {
		b.err = err
		return
	}
	if fieldBuf != nil {
		b.buf = append(b.buf, quoteCompositeFieldIfNeeded(string(fieldBuf))...)
	}

	b.buf = append(b.buf, ',')
}

func (b *CompositeTextBuilder) Finish() ([]byte, error) {
	if b.err != nil {
		return nil, b.err
	}

	b.buf[len(b.buf)-1] = ')'
	return b.buf, nil
}

var quoteCompositeReplacer = strings.NewReplacer(`\`, `\\`, `"`, `\"`)

func quoteCompositeField(src string) string {
	return `"` + quoteCompositeReplacer.Replace(src) + `"`
}

func quoteCompositeFieldIfNeeded(src string) string {
	if src == "" || src[0] == ' ' || src[len(src)-1] == ' ' || strings.ContainsAny(src, `(),"\`) {
		return quoteCompositeField(src)
	}
	return src
}

// CompositeFields represents the values of a composite value. It can be used as an encoding source or as a scan target.
// It cannot scan a NULL, but the composite fields can be NULL.
type CompositeFields []interface{}

func (cf CompositeFields) SkipUnderlyingTypePlan() {}

func (cf CompositeFields) IsNull() bool {
	return cf == nil
}

func (cf CompositeFields) Index(i int) interface{} {
	return cf[i]
}

func (cf CompositeFields) ScanNull() error {
	return fmt.Errorf("cannot scan NULL into CompositeFields")
}

func (cf CompositeFields) ScanIndex(i int) interface{} {
	return cf[i]
}
