package pgtype

import (
	"database/sql/driver"
	"errors"
	"fmt"
)

type PgLSN struct {
	LSN   uint64
	Valid bool
}

func parsePgLSN(s string) (uint64, error) {
	var hi, lo uint32
	n, err := fmt.Sscanf(s, "%X/%X", &hi, &lo)
	if err != nil {
		return 0, err
	}
	if n != 2 {
		return 0, errors.New("invalid pg_lsn value")
	}
	return uint64(hi)<<32 | uint64(lo), nil
}

func (src *PgLSN) String() string {
	if !src.Valid {
		return ""
	}
	return fmt.Sprintf("%X/%X", src.LSN>>32, uint32(src.LSN))
}

func (n *PgLSN) ScanUint64(v Uint64) error {
	*n = PgLSN{
		LSN:   v.Uint64,
		Valid: v.Valid,
	}
	return nil
}

func (n PgLSN) Uint64Value() (Uint64, error) {
	return Uint64{
		Uint64: n.LSN,
		Valid:  n.Valid,
	}, nil
}

type PgLSNCodec struct{}

func (PgLSNCodec) FormatSupported(format int16) bool {
	return format == TextFormatCode || format == BinaryFormatCode
}

func (PgLSNCodec) PreferredFormat() int16 {
	return TextFormatCode
}

func (c PgLSNCodec) PlanEncode(m *Map, oid uint32, format int16, value any) EncodePlan {
	switch format {
	case BinaryFormatCode:
		return Uint64Codec{}.PlanEncode(m, oid, format, value)
	case TextFormatCode:
		switch value.(type) {
		case PgLSN:
			return encodePlanPgLSNCodecEitherFormatPgLSN{}
		case string:
			return encodePlanPgLSNCodecEitherFormatString{}
		case []byte:
			return encodePlanPgLSNCodecEitherFormatByteSlice{}
		}

		// Because anything can be marshalled the normal wrapping in Map.PlanScan doesn't get a chance to run. So try the
		// appropriate wrappers here.
		for _, f := range []TryWrapEncodePlanFunc{
			TryWrapDerefPointerEncodePlan,
			TryWrapFindUnderlyingTypeEncodePlan,
		} {
			if wrapperPlan, nextValue, ok := f(value); ok {
				if nextPlan := c.PlanEncode(m, oid, format, nextValue); nextPlan != nil {
					wrapperPlan.SetNext(nextPlan)
					return wrapperPlan
				}
			}
		}
	}

	return nil
}

type encodePlanPgLSNCodecEitherFormatPgLSN struct{}

func (encodePlanPgLSNCodecEitherFormatPgLSN) Encode(value any, buf []byte) (newBuf []byte, err error) {
	v := value.(PgLSN)
	if !v.Valid {
		return nil, nil
	}

	pgLSNString := v.String()
	buf = append(buf, pgLSNString...)
	return buf, nil
}

type encodePlanPgLSNCodecEitherFormatString struct{}

func (encodePlanPgLSNCodecEitherFormatString) Encode(value any, buf []byte) (newBuf []byte, err error) {
	pgLSNString := value.(string)
	buf = append(buf, pgLSNString...)
	return buf, nil
}

type encodePlanPgLSNCodecEitherFormatByteSlice struct{}

func (encodePlanPgLSNCodecEitherFormatByteSlice) Encode(value any, buf []byte) (newBuf []byte, err error) {
	pgLSNBytes := value.([]byte)
	if pgLSNBytes == nil {
		return nil, nil
	}

	buf = append(buf, pgLSNBytes...)
	return buf, nil
}

func (PgLSNCodec) PlanScan(m *Map, oid uint32, format int16, target any) ScanPlan {
	switch format {
	case BinaryFormatCode:
		return Uint64Codec{}.PlanScan(m, oid, format, target)
	case TextFormatCode:
		switch target.(type) {
		case *PgLSN:
			return scanPlanPgLSNBytesToPgLSN{}
		case *string:
			return scanPlanAnyToString{}
		case *[]byte:
			return scanPlanPgLSNToByteSlice{}
		}
	}

	return nil
}

type scanPlanPgLSNBytesToPgLSN struct{}

func (scanPlanPgLSNBytesToPgLSN) Scan(src []byte, dst any) error {
	dstPgLSN := dst.(*PgLSN)
	if src == nil {
		*dstPgLSN = PgLSN{LSN: 0, Valid: false}
		return nil
	}

	n, err := parsePgLSN(string(src))
	if err != nil {
		return err
	}
	*dstPgLSN = PgLSN{LSN: n, Valid: true}
	return nil
}

type scanPlanPgLSNToByteSlice struct{}

func (scanPlanPgLSNToByteSlice) Scan(src []byte, dst any) error {
	dstBuf := dst.(*[]byte)
	if src == nil {
		*dstBuf = nil
		return nil
	}

	*dstBuf = make([]byte, len(src))
	copy(*dstBuf, src)
	return nil
}

func (PgLSNCodec) DecodeDatabaseSQLValue(m *Map, oid uint32, format int16, src []byte) (driver.Value, error) {
	if src == nil {
		return nil, nil
	}

	dstBuf := make([]byte, len(src))
	copy(dstBuf, src)
	return dstBuf, nil
}

func (PgLSNCodec) DecodeValue(tm *Map, oid uint32, format int16, src []byte) (any, error) {
	if src == nil {
		return nil, nil
	}

	var n uint64
	switch format {
	case BinaryFormatCode:
		an, err := Uint64Codec{}.DecodeValue(tm, oid, format, src)
		if err != nil {
			return nil, err
		}
		n = an.(uint64)
	case TextFormatCode:
		var err error
		n, err = parsePgLSN(string(src))
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unknown format code: %v", format)
	}

	return PgLSN{LSN: n, Valid: true}, nil
}
