package pgx

import (
	"database/sql/driver"
	"fmt"

	"github.com/jackc/pgx/v5/internal/anynil"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// ExtendedQueryBuilder is used to choose the parameter formats, to format the parameters and to choose the result
// formats for an extended query.
type ExtendedQueryBuilder struct {
	ParamValues     [][]byte
	paramValueBytes []byte
	ParamFormats    []int16
	ResultFormats   []int16
}

// Build sets ParamValues, ParamFormats, and ResultFormats for use with *PgConn.ExecParams or *PgConn.ExecPrepared. If
// sd is nil then QueryExecModeExec behavior will be used.
func (eqb *ExtendedQueryBuilder) Build(m *pgtype.Map, sd *pgconn.StatementDescription, args []any) error {
	eqb.reset()

	anynil.NormalizeSlice(args)

	if sd == nil {
		return eqb.appendParamsForQueryExecModeExec(m, args)
	}

	if len(sd.ParamOIDs) != len(args) {
		return fmt.Errorf("mismatched param and argument count")
	}

	for i := range args {
		err := eqb.appendParam(m, sd.ParamOIDs[i], -1, args[i])
		if err != nil {
			err = fmt.Errorf("failed to encode args[%d]: %v", i, err)
			return err
		}
	}

	for i := range sd.Fields {
		eqb.appendResultFormat(m.FormatCodeForOID(sd.Fields[i].DataTypeOID))
	}

	return nil
}

// appendParam appends a parameter to the query. format may be -1 to automatically choose the format. If arg is nil it
// must be an untyped nil.
func (eqb *ExtendedQueryBuilder) appendParam(m *pgtype.Map, oid uint32, format int16, arg any) error {
	if format == -1 {
		preferredFormat := eqb.chooseParameterFormatCode(m, oid, arg)
		preferredErr := eqb.appendParam(m, oid, preferredFormat, arg)
		if preferredErr == nil {
			return nil
		}

		var otherFormat int16
		if preferredFormat == TextFormatCode {
			otherFormat = BinaryFormatCode
		} else {
			otherFormat = TextFormatCode
		}

		otherErr := eqb.appendParam(m, oid, otherFormat, arg)
		if otherErr == nil {
			return nil
		}

		return preferredErr // return the error from the preferred format
	}

	v, err := eqb.encodeExtendedParamValue(m, oid, format, arg)
	if err != nil {
		return err
	}

	eqb.ParamFormats = append(eqb.ParamFormats, format)
	eqb.ParamValues = append(eqb.ParamValues, v)

	return nil
}

// appendResultFormat appends a result format to the query.
func (eqb *ExtendedQueryBuilder) appendResultFormat(format int16) {
	eqb.ResultFormats = append(eqb.ResultFormats, format)
}

// reset readies eqb to build another query.
func (eqb *ExtendedQueryBuilder) reset() {
	eqb.ParamValues = eqb.ParamValues[0:0]
	eqb.paramValueBytes = eqb.paramValueBytes[0:0]
	eqb.ParamFormats = eqb.ParamFormats[0:0]
	eqb.ResultFormats = eqb.ResultFormats[0:0]

	if cap(eqb.ParamValues) > 64 {
		eqb.ParamValues = make([][]byte, 0, 64)
	}

	if cap(eqb.paramValueBytes) > 256 {
		eqb.paramValueBytes = make([]byte, 0, 256)
	}

	if cap(eqb.ParamFormats) > 64 {
		eqb.ParamFormats = make([]int16, 0, 64)
	}
	if cap(eqb.ResultFormats) > 64 {
		eqb.ResultFormats = make([]int16, 0, 64)
	}
}

func (eqb *ExtendedQueryBuilder) encodeExtendedParamValue(m *pgtype.Map, oid uint32, formatCode int16, arg any) ([]byte, error) {
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
func (eqb *ExtendedQueryBuilder) chooseParameterFormatCode(m *pgtype.Map, oid uint32, arg any) int16 {
	switch arg.(type) {
	case string, *string:
		return TextFormatCode
	}

	return m.FormatCodeForOID(oid)
}

// appendParamsForQueryExecModeExec appends the args to eqb.
//
// Parameters must be encoded in the text format because of differences in type conversion between timestamps and
// dates. In QueryExecModeExec we don't know what the actual PostgreSQL type is. To determine the type we use the
// Go type to OID type mapping registered by RegisterDefaultPgType. However, the Go time.Time represents both
// PostgreSQL timestamp[tz] and date. To use the binary format we would need to also specify what the PostgreSQL
// type OID is. But that would mean telling PostgreSQL that we have sent a timestamp[tz] when what is needed is a date.
// This means that the value is converted from text to timestamp[tz] to date. This means it does a time zone conversion
// before converting it to date. This means that dates can be shifted by one day. In text format without that double
// type conversion it takes the date directly and ignores time zone (i.e. it works).
//
// Given that the whole point of QueryExecModeExec is to operate without having to know the PostgreSQL types there is
// no way to safely use binary or to specify the parameter OIDs.
func (eqb *ExtendedQueryBuilder) appendParamsForQueryExecModeExec(m *pgtype.Map, args []any) error {
	for _, arg := range args {
		if arg == nil {
			err := eqb.appendParam(m, 0, TextFormatCode, arg)
			if err != nil {
				return err
			}
		} else {
			dt, ok := m.TypeForValue(arg)
			if !ok {
				var tv pgtype.TextValuer
				if tv, ok = arg.(pgtype.TextValuer); ok {
					t, err := tv.TextValue()
					if err != nil {
						return err
					}

					dt, ok = m.TypeForOID(pgtype.TextOID)
					if ok {
						arg = t
					}
				}
			}
			if !ok {
				var dv driver.Valuer
				if dv, ok = arg.(driver.Valuer); ok {
					v, err := dv.Value()
					if err != nil {
						return err
					}
					dt, ok = m.TypeForValue(v)
					if ok {
						arg = v
					}
				}
			}
			if !ok {
				var str fmt.Stringer
				if str, ok = arg.(fmt.Stringer); ok {
					dt, ok = m.TypeForOID(pgtype.TextOID)
					if ok {
						arg = str.String()
					}
				}
			}
			if !ok {
				return &unknownArgumentTypeQueryExecModeExecError{arg: arg}
			}
			err := eqb.appendParam(m, dt.OID, TextFormatCode, arg)
			if err != nil {
				return err
			}
		}
	}

	return nil
}
