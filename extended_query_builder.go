package pgx

import (
	"github.com/jackc/pgx/v5/internal/anynil"
	"github.com/jackc/pgx/v5/pgtype"
)

type extendedQueryBuilder struct {
	paramValues     [][]byte
	paramValueBytes []byte
	paramFormats    []int16
	resultFormats   []int16
}

func (eqb *extendedQueryBuilder) AppendParam(m *pgtype.Map, oid uint32, arg any) error {
	f := eqb.chooseParameterFormatCode(m, oid, arg)
	return eqb.AppendParamFormat(m, oid, f, arg)
}

func (eqb *extendedQueryBuilder) AppendParamFormat(m *pgtype.Map, oid uint32, format int16, arg any) error {
	eqb.paramFormats = append(eqb.paramFormats, format)

	v, err := eqb.encodeExtendedParamValue(m, oid, format, arg)
	if err != nil {
		return err
	}
	eqb.paramValues = append(eqb.paramValues, v)

	return nil
}

func (eqb *extendedQueryBuilder) AppendResultFormat(f int16) {
	eqb.resultFormats = append(eqb.resultFormats, f)
}

// Reset readies eqb to build another query.
func (eqb *extendedQueryBuilder) Reset() {
	eqb.paramValues = eqb.paramValues[0:0]
	eqb.paramValueBytes = eqb.paramValueBytes[0:0]
	eqb.paramFormats = eqb.paramFormats[0:0]
	eqb.resultFormats = eqb.resultFormats[0:0]

	if cap(eqb.paramValues) > 64 {
		eqb.paramValues = make([][]byte, 0, 64)
	}

	if cap(eqb.paramValueBytes) > 256 {
		eqb.paramValueBytes = make([]byte, 0, 256)
	}

	if cap(eqb.paramFormats) > 64 {
		eqb.paramFormats = make([]int16, 0, 64)
	}
	if cap(eqb.resultFormats) > 64 {
		eqb.resultFormats = make([]int16, 0, 64)
	}
}

func (eqb *extendedQueryBuilder) encodeExtendedParamValue(m *pgtype.Map, oid uint32, formatCode int16, arg any) ([]byte, error) {
	if anynil.Is(arg) {
		return nil, nil
	}

	if eqb.paramValueBytes == nil {
		eqb.paramValueBytes = make([]byte, 0, 128)
	}

	pos := len(eqb.paramValueBytes)

	buf, err := m.Encode(oid, formatCode, arg, eqb.paramValueBytes)
	if err != nil {
		return nil, err
	}
	if buf == nil {
		return nil, nil
	}
	eqb.paramValueBytes = buf
	return eqb.paramValueBytes[pos:], nil
}

// chooseParameterFormatCode determines the correct format code for an
// argument to a prepared statement. It defaults to TextFormatCode if no
// determination can be made.
func (eqb *extendedQueryBuilder) chooseParameterFormatCode(m *pgtype.Map, oid uint32, arg any) int16 {
	switch arg.(type) {
	case string, *string:
		return TextFormatCode
	}

	return m.FormatCodeForOID(oid)
}
