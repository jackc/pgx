// Do not edit. Generated from pgtype/array_getter_setter.go.erb
package pgtype

type int16Array []int16

func (a int16Array) Dimensions() []ArrayDimension {
	if a == nil {
		return nil
	}

	return []ArrayDimension{{Length: int32(len(a)), LowerBound: 1}}
}

func (a int16Array) Index(i int) any {
	return a[i]
}

func (a int16Array) IndexType() any {
	var el int16
	return el
}

func (a *int16Array) SetDimensions(dimensions []ArrayDimension) error {
	if dimensions == nil {
		a = nil
		return nil
	}

	elementCount := cardinality(dimensions)
	*a = make(int16Array, elementCount)
	return nil
}

func (a int16Array) ScanIndex(i int) any {
	return &a[i]
}

func (a int16Array) ScanIndexType() any {
	return new(int16)
}

type uint16Array []uint16

func (a uint16Array) Dimensions() []ArrayDimension {
	if a == nil {
		return nil
	}

	return []ArrayDimension{{Length: int32(len(a)), LowerBound: 1}}
}

func (a uint16Array) Index(i int) any {
	return a[i]
}

func (a uint16Array) IndexType() any {
	var el uint16
	return el
}

func (a *uint16Array) SetDimensions(dimensions []ArrayDimension) error {
	if dimensions == nil {
		a = nil
		return nil
	}

	elementCount := cardinality(dimensions)
	*a = make(uint16Array, elementCount)
	return nil
}

func (a uint16Array) ScanIndex(i int) any {
	return &a[i]
}

func (a uint16Array) ScanIndexType() any {
	return new(uint16)
}
