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

func (n *PgLSN) ScanText(v Text) error {
	if v.Valid {
		i, err := parsePgLSN(v.String)
		if err != nil {
			return err
		}
		*n = PgLSN{
			LSN:   i,
			Valid: true,
		}
		return nil
	}

	*n = PgLSN{
		LSN:   0,
		Valid: false,
	}
	return nil
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

func (n PgLSN) TextValue() (Text, error) {
	return Text{
		String: n.String(),
		Valid:  n.Valid,
	}, nil
}

func (src *PgLSN) String() string {
	if !src.Valid {
		return ""
	}
	return fmt.Sprintf("%X/%X", src.LSN>>32, uint32(src.LSN))
}

// Scan implements the database/sql Scanner interface.
func (dst *PgLSN) Scan(src any) error {
	if src == nil {
		*dst = PgLSN{}
		return nil
	}

	var n uint64

	switch src := src.(type) {
	case int64:
		n = uint64(src)
	case uint64:
		n = src
	case string:
		var err error
		n, err = parsePgLSN(src)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("cannot scan %T", src)
	}

	*dst = PgLSN{LSN: n, Valid: true}

	return nil
}

// Value implements the database/sql/driver Valuer interface.
func (src PgLSN) Value() (driver.Value, error) {
	if !src.Valid {
		return nil, nil
	}
	// Return int64 instead of uint64 because uint64 is not allowed at
	// https://pkg.go.dev/database/sql/driver#Value
	return int64(src.LSN), nil
}

type PgLSNCodec struct {
	Uint64Codec
}

func (PgLSNCodec) DecodeValue(tm *Map, oid uint32, format int16, src []byte) (interface{}, error) {
	if src == nil {
		return nil, nil
	}

	var target PgLSN
	scanPlan := tm.PlanScan(oid, format, &target)
	if scanPlan == nil {
		return nil, fmt.Errorf("PlanScan did not find a plan")
	}

	err := scanPlan.Scan(src, &target)
	if err != nil {
		return nil, err
	}

	return target, nil
}
