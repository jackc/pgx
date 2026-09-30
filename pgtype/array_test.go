package pgtype

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseUntypedTextArray(t *testing.T) {
	tests := []struct {
		source string
		result untypedTextArray
	}{
		{
			source: "{}",
			result: untypedTextArray{
				Elements:   []string{},
				Quoted:     []bool{},
				Dimensions: []ArrayDimension{},
			},
		},
		{
			source: "{1}",
			result: untypedTextArray{
				Elements:   []string{"1"},
				Quoted:     []bool{false},
				Dimensions: []ArrayDimension{{Length: 1, LowerBound: 1}},
			},
		},
		{
			source: "{a,b}",
			result: untypedTextArray{
				Elements:   []string{"a", "b"},
				Quoted:     []bool{false, false},
				Dimensions: []ArrayDimension{{Length: 2, LowerBound: 1}},
			},
		},
		{
			source: `{"NULL"}`,
			result: untypedTextArray{
				Elements:   []string{"NULL"},
				Quoted:     []bool{true},
				Dimensions: []ArrayDimension{{Length: 1, LowerBound: 1}},
			},
		},
		{
			source: `{""}`,
			result: untypedTextArray{
				Elements:   []string{""},
				Quoted:     []bool{true},
				Dimensions: []ArrayDimension{{Length: 1, LowerBound: 1}},
			},
		},
		{
			source: `{"He said, \"Hello.\""}`,
			result: untypedTextArray{
				Elements:   []string{`He said, "Hello."`},
				Quoted:     []bool{true},
				Dimensions: []ArrayDimension{{Length: 1, LowerBound: 1}},
			},
		},
		{
			source: "{{a,b},{c,d},{e,f}}",
			result: untypedTextArray{
				Elements:   []string{"a", "b", "c", "d", "e", "f"},
				Quoted:     []bool{false, false, false, false, false, false},
				Dimensions: []ArrayDimension{{Length: 3, LowerBound: 1}, {Length: 2, LowerBound: 1}},
			},
		},
		{
			source: "{{{a,b},{c,d},{e,f}},{{a,b},{c,d},{e,f}}}",
			result: untypedTextArray{
				Elements: []string{"a", "b", "c", "d", "e", "f", "a", "b", "c", "d", "e", "f"},
				Quoted:   []bool{false, false, false, false, false, false, false, false, false, false, false, false},
				Dimensions: []ArrayDimension{
					{Length: 2, LowerBound: 1},
					{Length: 3, LowerBound: 1},
					{Length: 2, LowerBound: 1},
				},
			},
		},
		{
			source: "[4:4]={1}",
			result: untypedTextArray{
				Elements:   []string{"1"},
				Quoted:     []bool{false},
				Dimensions: []ArrayDimension{{Length: 1, LowerBound: 4}},
			},
		},
		{
			source: "[4:5][2:3]={{a,b},{c,d}}",
			result: untypedTextArray{
				Elements: []string{"a", "b", "c", "d"},
				Quoted:   []bool{false, false, false, false},
				Dimensions: []ArrayDimension{
					{Length: 2, LowerBound: 4},
					{Length: 2, LowerBound: 2},
				},
			},
		},
		{
			source: "[-4:-2]={1,2,3}",
			result: untypedTextArray{
				Elements:   []string{"1", "2", "3"},
				Quoted:     []bool{false, false, false},
				Dimensions: []ArrayDimension{{Length: 3, LowerBound: -4}},
			},
		},
	}

	for i, tt := range tests {
		r, err := parseUntypedTextArray(tt.source, ',')
		if err != nil {
			t.Errorf("%d: %v", i, err)
			continue
		}

		if !reflect.DeepEqual(*r, tt.result) {
			t.Errorf("%d: expected %+v to be parsed to %+v, but it was %+v", i, tt.source, tt.result, *r)
		}
	}
}

func TestParseUntypedTextArrayWithCustomDelimiter(t *testing.T) {
	r, err := parseUntypedTextArray(`{(2,2),(1,1);(4,4),(3,3)}`, ';')
	if err != nil {
		t.Fatal(err)
	}

	expected := untypedTextArray{
		Elements:   []string{"(2,2),(1,1)", "(4,4),(3,3)"},
		Quoted:     []bool{false, false},
		Dimensions: []ArrayDimension{{Length: 2, LowerBound: 1}},
	}

	if !reflect.DeepEqual(*r, expected) {
		t.Errorf("expected %+v, got %+v", expected, *r)
	}
}

func TestUintArrayRoundTrip(t *testing.T) {
	t.Parallel()

	testOIDs := map[string]uint32{
		"int2 array": Int2ArrayOID,
		"int4 array": Int4ArrayOID,
		"int8 array": Int8ArrayOID,
	}

	formats := map[string]int16{
		"binary format": BinaryFormatCode,
		"text format":   TextFormatCode,
	}

	for oidName, oid := range testOIDs {
		for formatName, format := range formats {
			name := oidName + " " + formatName
			t.Run(name, func(t *testing.T) {
				t.Parallel()

				m := NewMap()

				// Test uint16 (values within int2 range)
				want16 := []uint16{1, 42, 1342, 32767}
				buf16, err := m.Encode(oid, format, want16, nil)
				require.NoError(t, err)

				var got16 []uint16
				require.NoError(t, m.Scan(oid, format, buf16, &got16))
				assert.Equal(t, want16, got16)

				// Pointer to slice encode for uint16
				buf16Ptr, err := m.Encode(oid, format, &want16, nil)
				require.NoError(t, err)
				assert.Equal(t, buf16, buf16Ptr)

				if oid == Int2ArrayOID {
					return
				}

				// Test uint32 (values within int4 range)
				want32 := []uint32{1, 42, 1342, 65535, 1000000}
				buf32, err := m.Encode(oid, format, want32, nil)
				require.NoError(t, err)

				var got32 []uint32
				require.NoError(t, m.Scan(oid, format, buf32, &got32))
				assert.Equal(t, want32, got32)

				// Pointer to slice encode for uint32
				buf32Ptr, err := m.Encode(oid, format, &want32, nil)
				require.NoError(t, err)
				assert.Equal(t, buf32, buf32Ptr)

				// Test uint64
				want64 := []uint64{1, 42, 1342, 65535, 1000000}
				buf64, err := m.Encode(oid, format, want64, nil)
				require.NoError(t, err)

				var got64 []uint64
				require.NoError(t, m.Scan(oid, format, buf64, &got64))
				assert.Equal(t, want64, got64)

				// Pointer to slice encode for uint64
				buf64Ptr, err := m.Encode(oid, format, &want64, nil)
				require.NoError(t, err)
				assert.Equal(t, buf64, buf64Ptr)

				// Test uint
				wantUint := []uint{1, 42, 1342, 65535, 1000000}
				bufUint, err := m.Encode(oid, format, wantUint, nil)
				require.NoError(t, err)

				var gotUint []uint
				require.NoError(t, m.Scan(oid, format, bufUint, &gotUint))
				assert.Equal(t, wantUint, gotUint)

				// Pointer to slice encode for uint
				bufUintPtr, err := m.Encode(oid, format, &wantUint, nil)
				require.NoError(t, err)
				assert.Equal(t, bufUint, bufUintPtr)
			})
		}
	}
}

func TestUintArrayNullAndEmpty(t *testing.T) {
	t.Parallel()

	m := NewMap()

	for _, format := range []int16{BinaryFormatCode, TextFormatCode} {
		// Empty array
		empty := []uint64{}
		buf, err := m.Encode(Int4ArrayOID, format, empty, nil)
		require.NoError(t, err)

		var gotEmpty []uint64
		require.NoError(t, m.Scan(Int4ArrayOID, format, buf, &gotEmpty))
		assert.Empty(t, gotEmpty)

		// NULL array (nil buffer scan)
		gotNull := []uint64{1, 2, 3}
		require.NoError(t, m.Scan(Int4ArrayOID, format, nil, &gotNull))
		assert.Nil(t, gotNull)

		// Nil slice encode
		bufNil, err := m.Encode(Int4ArrayOID, format, ([]uint64)(nil), nil)
		require.NoError(t, err)
		assert.Nil(t, bufNil)

		// Nil pointer to slice encode
		bufNilPtr, err := m.Encode(Int4ArrayOID, format, (*[]uint64)(nil), nil)
		require.NoError(t, err)
		assert.Nil(t, bufNilPtr)
	}
}

func TestUintArrayPlansFastPath(t *testing.T) {
	t.Parallel()

	m := NewMap()

	// Verify ScanPlans use wrapPtrSliceScanPlan (fast path), NOT wrapPtrSliceReflectScanPlan
	plan16 := m.PlanScan(Int4ArrayOID, BinaryFormatCode, new([]uint16))
	require.NotNil(t, plan16)
	_, isFast16 := plan16.(*wrapPtrSliceScanPlan[uint16])
	assert.True(t, isFast16, "expected plan to be *wrapPtrSliceScanPlan[uint16], got %T", plan16)

	plan32 := m.PlanScan(Int4ArrayOID, BinaryFormatCode, new([]uint32))
	require.NotNil(t, plan32)
	_, isFast32 := plan32.(*wrapPtrSliceScanPlan[uint32])
	assert.True(t, isFast32, "expected plan to be *wrapPtrSliceScanPlan[uint32], got %T", plan32)

	plan64 := m.PlanScan(Int4ArrayOID, BinaryFormatCode, new([]uint64))
	require.NotNil(t, plan64)
	_, isFast64 := plan64.(*wrapPtrSliceScanPlan[uint64])
	assert.True(t, isFast64, "expected plan to be *wrapPtrSliceScanPlan[uint64], got %T", plan64)

	planUint := m.PlanScan(Int4ArrayOID, BinaryFormatCode, new([]uint))
	require.NotNil(t, planUint)
	_, isFastUint := planUint.(*wrapPtrSliceScanPlan[uint])
	assert.True(t, isFastUint, "expected plan to be *wrapPtrSliceScanPlan[uint], got %T", planUint)

	// Verify EncodePlans use wrapSliceEncodePlan (fast path), NOT wrapSliceEncodeReflectPlan
	encPlan16 := m.PlanEncode(Int4ArrayOID, BinaryFormatCode, []uint16{})
	require.NotNil(t, encPlan16)
	_, isFastEnc16 := encPlan16.(*wrapSliceEncodePlan[uint16])
	assert.True(t, isFastEnc16, "expected plan to be *wrapSliceEncodePlan[uint16], got %T", encPlan16)

	encPlan32 := m.PlanEncode(Int4ArrayOID, BinaryFormatCode, []uint32{})
	require.NotNil(t, encPlan32)
	_, isFastEnc32 := encPlan32.(*wrapSliceEncodePlan[uint32])
	assert.True(t, isFastEnc32, "expected plan to be *wrapSliceEncodePlan[uint32], got %T", encPlan32)

	encPlan64 := m.PlanEncode(Int4ArrayOID, BinaryFormatCode, []uint64{})
	require.NotNil(t, encPlan64)
	_, isFastEnc64 := encPlan64.(*wrapSliceEncodePlan[uint64])
	assert.True(t, isFastEnc64, "expected plan to be *wrapSliceEncodePlan[uint64], got %T", encPlan64)

	encPlanUint := m.PlanEncode(Int4ArrayOID, BinaryFormatCode, []uint{})
	require.NotNil(t, encPlanUint)
	_, isFastEncUint := encPlanUint.(*wrapSliceEncodePlan[uint])
	assert.True(t, isFastEncUint, "expected plan to be *wrapSliceEncodePlan[uint], got %T", encPlanUint)
	// Verify fast path for int, int8, bool, [16]byte
	planInt := m.PlanScan(Int4ArrayOID, BinaryFormatCode, new([]int))
	require.NotNil(t, planInt)
	_, isFastInt := planInt.(*wrapPtrSliceScanPlan[int])
	assert.True(t, isFastInt, "expected plan to be *wrapPtrSliceScanPlan[int], got %T", planInt)

	planInt8 := m.PlanScan(Int2ArrayOID, BinaryFormatCode, new([]int8))
	require.NotNil(t, planInt8)
	_, isFastInt8 := planInt8.(*wrapPtrSliceScanPlan[int8])
	assert.True(t, isFastInt8, "expected plan to be *wrapPtrSliceScanPlan[int8], got %T", planInt8)

	planBool := m.PlanScan(BoolArrayOID, BinaryFormatCode, new([]bool))
	require.NotNil(t, planBool)
	_, isFastBool := planBool.(*wrapPtrSliceScanPlan[bool])
	assert.True(t, isFastBool, "expected plan to be *wrapPtrSliceScanPlan[bool], got %T", planBool)

	planUUID := m.PlanScan(UUIDArrayOID, BinaryFormatCode, new([][16]byte))
	require.NotNil(t, planUUID)
	_, isFastUUID := planUUID.(*wrapPtrSliceScanPlan[[16]byte])
	assert.True(t, isFastUUID, "expected plan to be *wrapPtrSliceScanPlan[[16]byte], got %T", planUUID)

	encPlanInt := m.PlanEncode(Int4ArrayOID, BinaryFormatCode, []int{})
	require.NotNil(t, encPlanInt)
	_, isFastEncInt := encPlanInt.(*wrapSliceEncodePlan[int])
	assert.True(t, isFastEncInt, "expected plan to be *wrapSliceEncodePlan[int], got %T", encPlanInt)

	encPlanInt8 := m.PlanEncode(Int2ArrayOID, BinaryFormatCode, []int8{})
	require.NotNil(t, encPlanInt8)
	_, isFastEncInt8 := encPlanInt8.(*wrapSliceEncodePlan[int8])
	assert.True(t, isFastEncInt8, "expected plan to be *wrapSliceEncodePlan[int8], got %T", encPlanInt8)

	encPlanBool := m.PlanEncode(BoolArrayOID, BinaryFormatCode, []bool{})
	require.NotNil(t, encPlanBool)
	_, isFastEncBool := encPlanBool.(*wrapSliceEncodePlan[bool])
	assert.True(t, isFastEncBool, "expected plan to be *wrapSliceEncodePlan[bool], got %T", encPlanBool)

	encPlanUUID := m.PlanEncode(UUIDArrayOID, BinaryFormatCode, [][16]byte{})
	require.NotNil(t, encPlanUUID)
	_, isFastEncUUID := encPlanUUID.(*wrapSliceEncodePlan[[16]byte])
	assert.True(t, isFastEncUUID, "expected plan to be *wrapSliceEncodePlan[[16]byte], got %T", encPlanUUID)
}

func TestExtendedArrayRoundTrip(t *testing.T) {
	t.Parallel()

	m := NewMap()

	for _, format := range []int16{BinaryFormatCode, TextFormatCode} {
		// Test int
		wantInt := []int{-42, 0, 1, 1342, 100000}
		bufInt, err := m.Encode(Int4ArrayOID, format, wantInt, nil)
		require.NoError(t, err)
		var gotInt []int
		require.NoError(t, m.Scan(Int4ArrayOID, format, bufInt, &gotInt))
		assert.Equal(t, wantInt, gotInt)

		bufIntPtr, err := m.Encode(Int4ArrayOID, format, &wantInt, nil)
		require.NoError(t, err)
		assert.Equal(t, bufInt, bufIntPtr)

		// Test int8 (on int2 array)
		wantInt8 := []int8{-128, -1, 0, 1, 127}
		bufInt8, err := m.Encode(Int2ArrayOID, format, wantInt8, nil)
		require.NoError(t, err)
		var gotInt8 []int8
		require.NoError(t, m.Scan(Int2ArrayOID, format, bufInt8, &gotInt8))
		assert.Equal(t, wantInt8, gotInt8)

		bufInt8Ptr, err := m.Encode(Int2ArrayOID, format, &wantInt8, nil)
		require.NoError(t, err)
		assert.Equal(t, bufInt8, bufInt8Ptr)

		// Test bool
		wantBool := []bool{true, false, true, true, false}
		bufBool, err := m.Encode(BoolArrayOID, format, wantBool, nil)
		require.NoError(t, err)
		var gotBool []bool
		require.NoError(t, m.Scan(BoolArrayOID, format, bufBool, &gotBool))
		assert.Equal(t, wantBool, gotBool)

		bufBoolPtr, err := m.Encode(BoolArrayOID, format, &wantBool, nil)
		require.NoError(t, err)
		assert.Equal(t, bufBool, bufBoolPtr)

		// Test [16]byte (UUID)
		wantUUID := [][16]byte{
			{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			{15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0},
		}
		bufUUID, err := m.Encode(UUIDArrayOID, format, wantUUID, nil)
		require.NoError(t, err)
		var gotUUID [][16]byte
		require.NoError(t, m.Scan(UUIDArrayOID, format, bufUUID, &gotUUID))
		assert.Equal(t, wantUUID, gotUUID)

		bufUUIDPtr, err := m.Encode(UUIDArrayOID, format, &wantUUID, nil)
		require.NoError(t, err)
		assert.Equal(t, bufUUID, bufUUIDPtr)
	}
}

func BenchmarkUint64ArrayScan(b *testing.B) {
	colos := make([]int32, 500)
	for i := range colos {
		colos[i] = int32(i + 1)
	}

	m := NewMap()
	buf, err := m.Encode(Int4ArrayOID, BinaryFormatCode, colos, nil)
	require.NoError(b, err)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		var dst []uint64
		if err := m.Scan(Int4ArrayOID, BinaryFormatCode, buf, &dst); err != nil {
			b.Fatal(err)
		}
	}
}
