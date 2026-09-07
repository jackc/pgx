package pgtype

import (
	"slices"
	"testing"
)

// fuzzScanMap returns a Map covering every default-registered codec plus a few
// that require manual registration, and the sorted list of registered OIDs.
func fuzzScanMap() (*Map, []uint32) {
	m := NewMap()

	// These codecs are not registered by default. The OIDs are arbitrary
	// values that do not collide with any default-registered OID.
	m.RegisterType(&Type{Name: "hstore", OID: 999901, Codec: HstoreCodec{}})
	m.RegisterType(&Type{Name: "fuzz_enum", OID: 999902, Codec: &EnumCodec{}})
	int4Type, _ := m.TypeForOID(Int4OID)
	textType, _ := m.TypeForOID(TextOID)
	m.RegisterType(&Type{Name: "fuzz_composite", OID: 999903, Codec: &CompositeCodec{
		Fields: []CompositeCodecField{
			{Name: "a", Type: int4Type},
			{Name: "b", Type: textType},
		},
	}})

	// A Map starts empty and falls back to the default type map, so enumerate
	// both to cover every codec reachable through m.
	oidSet := make(map[uint32]struct{})
	for oid := range defaultMap.oidToType {
		oidSet[oid] = struct{}{}
	}
	for oid := range m.oidToType {
		oidSet[oid] = struct{}{}
	}
	oids := make([]uint32, 0, len(oidSet))
	for oid := range oidSet {
		oids = append(oids, oid)
	}
	slices.Sort(oids)
	return m, oids
}

// FuzzPlanScan feeds arbitrary bytes to the scan path of every registered
// codec, in both the text and binary formats. Malformed input — truncated
// messages, length prefixes exceeding the remaining bytes, huge element
// counts — must produce an error, never a panic. The corpus is seeded with
// inputs modeled on previously fixed decoder panics.
func FuzzPlanScan(f *testing.F) {
	m, oids := fuzzScanMap()

	seed := func(oid uint32, format int16, data []byte) {
		idx := slices.Index(oids, oid)
		if idx < 0 {
			f.Fatalf("seed OID %d is not registered", oid)
		}
		f.Add(uint16(idx), byte(format), data)
	}

	// Truncated headers.
	seed(Int4multirangeOID, BinaryFormatCode, []byte{0, 0, 0})
	seed(Int4ArrayOID, BinaryFormatCode, []byte{0, 0, 0, 1, 0, 0, 0, 0})
	seed(RecordOID, BinaryFormatCode, []byte{0, 0, 0, 1, 0, 0})
	seed(999901, BinaryFormatCode, []byte{0, 0, 0, 1, 0, 0, 0, 5})
	// Element counts and length prefixes exceeding the message.
	seed(Int4multirangeOID, BinaryFormatCode, []byte{0xff, 0xff, 0xff, 0xff})
	seed(Int4ArrayOID, BinaryFormatCode, []byte{
		0, 0, 0, 1, // 1 dimension
		0, 0, 0, 0, // no NULLs
		0, 0, 0, 0x17, // element OID int4
		0x7f, 0xff, 0xff, 0xff, // dimension length
		0, 0, 0, 1, // lower bound
	})
	seed(Int4rangeOID, BinaryFormatCode, []byte{0x02, 0xff, 0xff, 0xff, 0x7f})
	seed(TSVectorOID, BinaryFormatCode, []byte{0xff, 0xff, 0xff, 0x7f, 'a'})
	seed(VarbitOID, BinaryFormatCode, []byte{0xff, 0xff, 0xff, 0x7f, 0x01})
	seed(999903, BinaryFormatCode, []byte{0, 0, 0, 2, 0, 0, 0, 0x17, 0xff, 0xff, 0xff, 0x7f})
	// Text-format parsers with hand-rolled tokenizers.
	seed(Int4ArrayOID, TextFormatCode, []byte(`{{1,2},{3}`))
	seed(IntervalOID, TextFormatCode, []byte(`1 -2:03:`))
	seed(PointOID, TextFormatCode, []byte(`(1,`))
	seed(NumericOID, TextFormatCode, []byte(`1e+`))

	f.Fuzz(func(t *testing.T, oidIdx uint16, format byte, data []byte) {
		oid := oids[int(oidIdx)%len(oids)]
		formatCode := int16(format % 2)

		for _, target := range []any{new(any), new(string), new([]byte)} {
			plan := m.PlanScan(oid, formatCode, target)
			if plan == nil {
				continue
			}
			_ = plan.Scan(data, target)
		}
	})
}
