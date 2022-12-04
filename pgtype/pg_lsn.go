package pgtype

import "fmt"

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
		Valid:  true,
	}, nil
}

func (src *PgLSN) String() string {
	if !src.Valid {
		return ""
	}
	return fmt.Sprintf("%X/%X", src.LSN>>32, uint32(src.LSN))
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
