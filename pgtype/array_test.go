package pgtype

import (
	"reflect"
	"testing"
)

func TestFlatArraySetDimensions(t *testing.T) {
	t.Run("NULL array", func(t *testing.T) {
		value := 1
		storage := FlatArray[*int]{&value}
		backing := storage

		err := backing.SetDimensions(nil)
		if err != nil {
			t.Fatal(err)
		}

		if backing != nil {
			t.Fatalf("expected nil array, got %v", backing)
		}
		// Setting the destination to nil must not mutate storage that may still be referenced by an alias.
		if storage[0] != &value {
			t.Fatal("cleared aliased storage while setting a NULL array")
		}
	})

	t.Run("empty array from nil", func(t *testing.T) {
		var backing FlatArray[int]

		err := backing.SetDimensions([]ArrayDimension{})
		if err != nil {
			t.Fatal(err)
		}

		// A non-NULL empty PostgreSQL array must remain distinguishable from NULL.
		if backing == nil {
			t.Fatal("expected empty array to be non-nil")
		}
		if len(backing) != 0 {
			t.Fatalf("expected length 0, got %d", len(backing))
		}
	})

	t.Run("empty array reuses and clears storage", func(t *testing.T) {
		storage := FlatArray[int]{1, 2}
		backing := storage

		err := backing.SetDimensions([]ArrayDimension{})
		if err != nil {
			t.Fatal(err)
		}

		if backing == nil || len(backing) != 0 {
			t.Fatalf("expected non-nil empty array, got %#v", backing)
		}
		if storage[0] != 0 || storage[1] != 0 {
			t.Fatalf("expected removed elements to be cleared, got %v", storage)
		}
	})

	t.Run("same length reuses and clears storage", func(t *testing.T) {
		backing := FlatArray[int]{1, 2}
		originalElement := &backing[0]

		err := backing.SetDimensions([]ArrayDimension{{Length: 2, LowerBound: 1}})
		if err != nil {
			t.Fatal(err)
		}

		if originalElement != &backing[0] {
			t.Fatal("expected existing capacity to be reused")
		}
		if !reflect.DeepEqual(backing, FlatArray[int]{0, 0}) {
			t.Fatalf("expected elements to be cleared, got %v", backing)
		}
	})

	t.Run("grow within capacity", func(t *testing.T) {
		first, second, third, outsideDestination := 1, 2, 3, 4
		storage := FlatArray[*int]{&first, &second, &third, &outsideDestination}
		// Expose only the first two elements while retaining capacity for all four. Growing backing to three elements should
		// reuse storage and clear the resized destination without touching storage[3], which may belong to another slice.
		backing := storage[:2]
		originalElement := &backing[0]

		err := backing.SetDimensions([]ArrayDimension{{Length: 3, LowerBound: 1}})
		if err != nil {
			t.Fatal(err)
		}

		if len(backing) != 3 {
			t.Fatalf("expected length 3, got %d", len(backing))
		}
		if originalElement != &backing[0] {
			t.Fatal("expected existing capacity to be reused")
		}
		for i, element := range backing {
			if element != nil {
				t.Fatalf("expected element %d to be cleared, got %v", i, element)
			}
		}
		if storage[3] != &outsideDestination {
			t.Fatal("cleared an element outside the resized destination")
		}
	})

	t.Run("shrink within capacity", func(t *testing.T) {
		first, second, third, outsideDestination := 1, 2, 3, 4
		storage := FlatArray[*int]{&first, &second, &third, &outsideDestination}
		backing := storage[:3]
		originalElement := &backing[0]

		err := backing.SetDimensions([]ArrayDimension{{Length: 1, LowerBound: 1}})
		if err != nil {
			t.Fatal(err)
		}

		if len(backing) != 1 {
			t.Fatalf("expected length 1, got %d", len(backing))
		}
		if originalElement != &backing[0] {
			t.Fatal("expected existing capacity to be reused")
		}
		// Clear the full old destination so truncated pointer-bearing elements do not retain references.
		for i, element := range storage[:3] {
			if element != nil {
				t.Fatalf("expected old element %d to be cleared, got %v", i, element)
			}
		}
		if storage[3] != &outsideDestination {
			t.Fatal("cleared an element outside the old destination")
		}
	})

	t.Run("insufficient capacity allocates", func(t *testing.T) {
		value := 1
		storage := FlatArray[*int]{&value}
		backing := storage[:1:1]
		originalElement := &backing[0]

		err := backing.SetDimensions([]ArrayDimension{{Length: 2, LowerBound: 1}})
		if err != nil {
			t.Fatal(err)
		}

		if len(backing) != 2 {
			t.Fatalf("expected length 2, got %d", len(backing))
		}
		if originalElement == &backing[0] {
			t.Fatal("expected a new allocation when capacity is insufficient")
		}
		if backing[0] != nil || backing[1] != nil {
			t.Fatalf("expected new elements to be cleared, got %v", backing)
		}
		// Allocating a replacement must not mutate storage retained by an alias.
		if storage[0] != &value {
			t.Fatal("cleared aliased storage while allocating a replacement")
		}
	})

	t.Run("multidimensional cardinality", func(t *testing.T) {
		var backing FlatArray[int]

		err := backing.SetDimensions([]ArrayDimension{
			{Length: 2, LowerBound: -1},
			{Length: 3, LowerBound: 4},
		})
		if err != nil {
			t.Fatal(err)
		}

		if len(backing) != 6 {
			t.Fatalf("expected flattened length 6, got %d", len(backing))
		}
		if !reflect.DeepEqual(backing, FlatArray[int]{0, 0, 0, 0, 0, 0}) {
			t.Fatalf("expected elements to be cleared, got %v", backing)
		}
	})
}

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
		r, err := parseUntypedTextArray(tt.source)
		if err != nil {
			t.Errorf("%d: %v", i, err)
			continue
		}

		if !reflect.DeepEqual(*r, tt.result) {
			t.Errorf("%d: expected %+v to be parsed to %+v, but it was %+v", i, tt.source, tt.result, *r)
		}
	}
}
