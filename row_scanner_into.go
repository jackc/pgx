package pgx

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

// RowScannerInto provides an interface for scanning a row into a receiver value,
// while doing type-checking and initialization only once per query.
type RowScannerInto[T any] interface {
	// Initialize sets up the RowScannerInto and validates it against the rows.
	// Initialize must be called once before ScanRowInto.
	Initialize(rows Rows) error
	// ScanRowInto scans the row into the receiver.
	ScanRowInto(receiver *T, rows Rows) error
	// TODO: This could include a "Release" method, which could be used to pool
	// scanner objects, and any internal objects they store, between queries.
}

// RowIntoSpec defines a specification for scanning rows into a given type.
//
// Note on the weird type definitions:
// RowIntoSpec returns a struct containing a private function pointer because:
//  1. We want to be able to manage the lifecycle of the returned value inside the
//     collection functions. (E.g. we may decide to pool scanners for reuse.)
//     In order to do this safely, we need to ensure the RowScannerInto returned by
//     the inner function isn't referenced outside of the collecting function.
//  2. Returning a struct allows us to extend this value in the future if necessary.
//     By comparison, returning a function would not, and would require a (technically)
//     breaking change if the type needed to change in the future.
//  3. Returning a non-exported type lets us hide as many details as possible from
//     the public API and restrict the only valid usage to:
//     pgx.CollectRowsUsing(rows, RowInto[Type])
//  4. RowIntoSpec is itself a function to provide a place to put the generic type
//     parameter. rowIntoSpecRes cannot be a constant, since then there would be no
//     place to put the type parameter. Since rowIntoSpecRes cannot be constructed in
//     client code (by desing) it can't be applied when creating a struct value.
type RowIntoSpec[T any] func() rowIntoSpecRes[T]

type rowIntoSpecRes[T any] struct {
	fn func() RowScannerInto[T]
}

// AppendRowsUsing iterates through rows, scanning each row according to into,
// and appending the results into a slice of T.
func AppendRowsUsing[T any, S ~[]T](slice S, rows Rows, into RowIntoSpec[T]) (S, error) {
	return AppendRowsUsingScanner(slice, rows, into().fn())
}

// AppendRowsUsingScanner iterates through rows, scanning each row with the scanner,
// and appending the results into a slice of T.
func AppendRowsUsingScanner[T any, S ~[]T](
	slice S,
	rows Rows,
	scanner RowScannerInto[T],
) (s S, err error) {
	defer rows.Close()

	if err := scanner.Initialize(rows); err != nil {
		return nil, err
	}

	startingLen := len(slice)
	var startingPtr *T
	if cap(slice) > 0 {
		startingPtr = &slice[:cap(slice)][0]
	}

	defer func() {
		// Named return values guarantee this err is the err that's actually returned.
		if err != nil && len(slice) > startingLen && &slice[0] == startingPtr {
			// An error occurred AND slice still has the same backing array as the input slice.
			// Therefore, some written values are visible in the input slice. This could cause
			// problems, especially if T contains pointers which are kept alive.
			// To mitigate this, zero out the slice beyond the starting length.
			for i := range slice[startingLen:] {
				var zero T
				slice[startingLen+i] = zero
			}
		}
	}()

	for rows.Next() {
		i := len(slice)
		var zero T
		slice = append(slice, zero)
		err := scanner.ScanRowInto(&slice[i], rows)
		if err != nil {
			return nil, err
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return slice, nil
}

// CollectRowsUsing iterates through rows, scanning each row according to into,
// and collecting the results into a slice of T.
func CollectRowsUsing[T any](rows Rows, into RowIntoSpec[T]) ([]T, error) {
	return CollectRowsUsingScanner(rows, into().fn())
}

// CollectRowsUsingScanner iterates through rows, scanning each row with the scanner,
// and collecting the results into a slice of T.
func CollectRowsUsingScanner[T any](rows Rows, scanner RowScannerInto[T]) ([]T, error) {
	return AppendRowsUsingScanner([]T{}, rows, scanner)
}

// CollectOneRowUsing scans the first row in rows and returns the result. If no rows are found returns an error where errors.Is(ErrNoRows) is true.
// CollectOneRowUsing is to CollectRowsUsing as QueryRow is to Query.
func CollectOneRowUsing[T any](rows Rows, into RowIntoSpec[T]) (T, error) {
	return CollectOneRowUsingScannner(rows, into().fn())
}

// CollectOneRowUsingScanner scans the first row in rows and returns the result. If no rows are found returns an error where errors.Is(ErrNoRows) is true.
// CollectOneRowUsingScanner is to CollectRowsUsingScanner as QueryRow is to Query.
func CollectOneRowUsingScannner[T any](rows Rows, scanner RowScannerInto[T]) (T, error) {
	defer rows.Close()

	var (
		err   error
		value T
		zero  T
	)

	err = scanner.Initialize(rows)
	if err != nil {
		return zero, err
	}

	if !rows.Next() {
		if err = rows.Err(); err != nil {
			return zero, err
		}
		return zero, ErrNoRows
	}

	err = scanner.ScanRowInto(&value, rows)
	if err != nil {
		return zero, err
	}

	err = rows.Err()
	if err != nil {
		return zero, err
	}

	return value, nil
}

// CollectExactlyOneRowUsing scans the first row in rows and returns the result.
//   - If no rows are found returns an error where errors.Is(ErrNoRows) is true.
//   - If more than 1 row is found returns an error where errors.Is(ErrTooManyRows) is true.
func CollectExactlyOneRowUsing[T any](rows Rows, into RowIntoSpec[T]) (T, error) {
	return CollectExactlyOneRowUsingScanner(rows, into().fn())
}

// CollectExactlyOneRowUsingScanner scans the first row in rows and returns the result.
//   - If no rows are found returns an error where errors.Is(ErrNoRows) is true.
//   - If more than 1 row is found returns an error where errors.Is(ErrTooManyRows) is true.
func CollectExactlyOneRowUsingScanner[T any](rows Rows, scanner RowScannerInto[T]) (T, error) {
	defer rows.Close()

	var (
		err   error
		value T
		zero  T
	)

	err = scanner.Initialize(rows)
	if err != nil {
		return zero, err
	}

	if !rows.Next() {
		if err = rows.Err(); err != nil {
			return zero, err
		}

		return zero, ErrNoRows
	}

	err = scanner.ScanRowInto(&value, rows)
	if err != nil {
		return zero, err
	}

	if rows.Next() {
		return zero, ErrTooManyRows
	}

	err = rows.Err()
	if err != nil {
		return zero, err
	}

	return value, nil
}

type simpleRowScannerInto[T any] struct {
	scanTargets []any
}

var _ RowScannerInto[struct{}] = (*simpleRowScannerInto[struct{}])(nil)

// NewSimpleRowScannerInto returns a RowScannerInto that scans a row into a T.
func NewSimpleRowScannerInto[T any]() RowScannerInto[T] {
	return &simpleRowScannerInto[T]{}
}

// NewAddrOfSimpleRowScannerInto returns a RowScannerInto that scans a row into a *T.
func NewAddrOfSimpleRowScannerInto[T any]() RowScannerInto[*T] {
	return NewAddrScannerInto(NewSimpleRowScannerInto[T]())
}

// RowInto scans a row into a T.
func RowInto[T any]() rowIntoSpecRes[T] {
	return rowIntoSpecRes[T]{fn: NewSimpleRowScannerInto[T]}
}

// RowIntoAddrOf scans a row into a *T.
func RowIntoAddrOf[T any]() rowIntoSpecRes[*T] {
	return rowIntoSpecRes[*T]{fn: NewAddrOfSimpleRowScannerInto[T]}
}

func (rs *simpleRowScannerInto[T]) Initialize(rows Rows) error {
	return nil
}

func (rs *simpleRowScannerInto[T]) ScanRowInto(receiver *T, rows Rows) error {
	if rs.scanTargets == nil {
		rs.scanTargets = make([]any, 1)
	}
	rs.scanTargets[0] = receiver
	return rows.Scan(rs.scanTargets...)
}

// structRowField describes a field of a struct.
//
// TODO: It would be a bit more efficient to track the path using the pointer
// offset within the (outermost) struct and use unsafe.Pointer arithmetic to
// construct references when scanning rows. However, it's not clear it's worth
// using unsafe for this.
type structRowField struct {
	path []int
}

type positionalStructRowScannerInto[T any] struct {
	structRowScannerInto[T]
}

var _ RowScannerInto[struct{}] = (*positionalStructRowScannerInto[struct{}])(nil)

// NewPositionalStructRowScannerInto returns a RowScannerInto that scans a T from a row.
// T must be a struct. T must have the same number of public fields as row has fields.
// The row and T fields will be matched by position.
// If the "db" struct tag is "-" then the field will be ignored.
func NewPositionalStructRowScannerInto[T any]() RowScannerInto[T] {
	return &positionalStructRowScannerInto[T]{}
}

// NewPositionalStructRowScannerInto returns a RowScannerInto that scans a *T from a row.
// T must be a struct. T must have the same number of public fields as row has fields.
// The row and T fields will be matched by position.
// If the "db" struct tag is "-" then the field will be ignored.
func NewAddrOfPositionalStructRowScannerInto[T any]() RowScannerInto[*T] {
	return NewAddrScannerInto[T](NewPositionalStructRowScannerInto[T]())
}

// RowIntoStructByPos scans a row into a T.
// T must be a struct. T must have the same number of public fields as row has fields.
// The row and T fields will be matched by position.
// If the "db" struct tag is "-" then the field will be ignored.
func RowIntoStructByPos[T any]() rowIntoSpecRes[T] {
	return rowIntoSpecRes[T]{fn: NewPositionalStructRowScannerInto[T]}
}

// RowIntoStructByPos scans a row into a *T.
// T must be a struct. T must have the same number of public fields as row has fields.
// The row and T fields will be matched by position.
// If the "db" struct tag is "-" then the field will be ignored.
func RowIntoAddrOfStructByPos[T any]() rowIntoSpecRes[*T] {
	return rowIntoSpecRes[*T]{fn: NewAddrOfPositionalStructRowScannerInto[T]}
}

func (rs *positionalStructRowScannerInto[T]) Initialize(rows Rows) error {
	typ := reflect.TypeFor[T]()
	if typ.Kind() != reflect.Struct {
		return fmt.Errorf("generic type '%s' is not a struct", typ.Name())
	}
	fldDescs := rows.FieldDescriptions()
	rs.fields = make([]structRowField, 0, len(fldDescs))
	rs.populateFields(typ, &[]int{})
	if len(rs.fields) != len(fldDescs) {
		return fmt.Errorf(
			"got %d fields, but dst struct has only %d fields",
			len(rows.RawValues()),
			len(rs.fields),
		)
	}
	return nil
}

func (rs *positionalStructRowScannerInto[T]) populateFields(t reflect.Type, fieldStack *[]int) {
	// TODO: The mapping from t -> fields is static. We can do this just once per type and cache
	// the value to avoid re-computing the fields for each query.
	tail := len(*fieldStack)
	*fieldStack = append(*fieldStack, 0)
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		(*fieldStack)[tail] = i
		// Handle anonymous struct embedding, but do not try to handle embedded pointers.
		if sf.Anonymous && sf.Type.Kind() == reflect.Struct {
			rs.populateFields(sf.Type, fieldStack)
		} else if sf.PkgPath == "" {
			dbTag, _ := sf.Tag.Lookup(structTagKey)
			if dbTag == "-" {
				// Field is ignored, skip it.
				continue
			}
			rs.fields = append(rs.fields, structRowField{
				path: append([]int(nil), *fieldStack...),
			})
		}
	}
	*fieldStack = (*fieldStack)[:tail]
}

type namedStructRowScannerInto[T any] struct {
	structRowScannerInto[T]
	lax bool
}

var _ RowScannerInto[struct{}] = (*namedStructRowScannerInto[struct{}])(nil)

// NewNamedStructRowScannerInto returns RowScannerInto that scans a row into a T.
// T must be a struct. T must have the same number of named public fields as row has fields.
// The row and T fields will be matched by name. The match is case-insensitive.
// The database column name can be overridden with a "db" struct tag.
// If the "db" struct tag is "-" then the field will be ignored.
func NewNamedStructRowScannerInto[T any]() RowScannerInto[T] {
	return &namedStructRowScannerInto[T]{}
}

// NewLaxNamedStructRowScannerInto returns RowScannerInto that scans a row into a T.
// T must be a struct. T must have greater than or equal number of named public fields as row has fields.
// The row and T fields will be matched by name. The match is case-insensitive.
// The database column name can be overridden with a "db" struct tag.
// If the "db" struct tag is "-" then the field will be ignored.
func NewLaxNamedStructRowScannerInto[T any]() RowScannerInto[T] {
	return &namedStructRowScannerInto[T]{lax: true}
}

// NewAddrOfNamedStructRowScannerInto returns RowScannerInto that scans a row into a *T.
// T must be a struct. T must have the same number of named public fields as row has fields.
// The row and T fields will be matched by name. The match is case-insensitive.
// The database column name can be overridden with a "db" struct tag.
// If the "db" struct tag is "-" then the field will be ignored.
func NewAddrOfNamedStructRowScannerInto[T any]() RowScannerInto[*T] {
	return NewAddrScannerInto[T](NewNamedStructRowScannerInto[T]())
}

// NewAddrOfLaxNamedStructRowScannerInto returns RowScannerInto that scans a row into a *T.
// T must be a struct. T must have greater than or equal number of named public fields as row has fields.
// The row and T fields will be matched by name. The match is case-insensitive.
// The database column name can be overridden with a "db" struct tag.
// If the "db" struct tag is "-" then the field will be ignored.
func NewAddrOfLaxNamedStructRowScannerInto[T any]() RowScannerInto[*T] {
	return NewAddrScannerInto[T](NewNamedStructRowScannerInto[T]())
}

// RowIntoStructByName scans a row into a T.
// T must be a struct. T must have the same number of named public fields as row has fields.
// The row and T fields will be matched by name. The match is case-insensitive.
// The database column name can be overridden with a "db" struct tag.
// If the "db" struct tag is "-" then the field will be ignored.
func RowIntoStructByName[T any]() rowIntoSpecRes[T] {
	return rowIntoSpecRes[T]{fn: NewNamedStructRowScannerInto[T]}
}

// RowIntoAddrOfStructByName scans a row into a *T.
// T must be a struct. T must have the same number of named public fields as row has fields.
// The row and T fields will be matched by name. The match is case-insensitive.
// The database column name can be overridden with a "db" struct tag.
// If the "db" struct tag is "-" then the field will be ignored.
func RowIntoAddrOfStructByName[T any]() rowIntoSpecRes[*T] {
	return rowIntoSpecRes[*T]{fn: NewAddrOfNamedStructRowScannerInto[T]}
}

// RowIntoStructByNameLax scans a row into a T.
// T must be a struct. T must have greater than or equal number of named public fields as row has fields.
// The row and T fields will be matched by name. The match is case-insensitive.
// The database column name can be overridden with a "db" struct tag.
// If the "db" struct tag is "-" then the field will be ignored.
func RowIntoStructByNameLax[T any]() rowIntoSpecRes[T] {
	return rowIntoSpecRes[T]{fn: NewNamedStructRowScannerInto[T]}
}

// RowIntoAddrOfStructByNameLax scans a row into a *T.
// T must be a struct. T must have greater than or equal number of named public fields as row has fields.
// The row and T fields will be matched by name. The match is case-insensitive.
// The database column name can be overridden with a "db" struct tag.
// If the "db" struct tag is "-" then the field will be ignored.
func RowIntoAddrOfStructByNameLax[T any]() rowIntoSpecRes[*T] {
	return rowIntoSpecRes[*T]{fn: NewAddrOfNamedStructRowScannerInto[T]}
}

func (rs *namedStructRowScannerInto[T]) Initialize(rows Rows) error {
	typ := reflect.TypeFor[T]()
	if typ.Kind() != reflect.Struct {
		return fmt.Errorf("generic type '%s' is not a struct", typ.Name())
	}
	fldDescs := rows.FieldDescriptions()
	rs.fields = make([]structRowField, len(fldDescs))
	err := rs.populateFields(fldDescs, typ, &[]int{})
	if err != nil {
		return err
	}

	for i, f := range rs.fields {
		if f.path == nil {
			return fmt.Errorf(
				"struct doesn't have corresponding row field %s",
				rows.FieldDescriptions()[i].Name,
			)
		}
	}

	return nil
}

func (rs *namedStructRowScannerInto[T]) populateFields(
	fldDescs []pgconn.FieldDescription,
	t reflect.Type,
	fieldStack *[]int,
) error {
	// TODO: The mapping from (t, fldDescs) -> fields is static. We can do this just once
	// per type / field-list and cache the value to avoid re-computing the fields for each query.
	// However, this is slightly harder than in the positional scanner because it's we need an
	// immutable (and ideally small and cheaply comparable) representation of the field-set.
	// Joining the field names with a character that's invalid in postgresql column names could
	// work, but it is not bounded in size. Regardless, it's still probaby cheaper than re-running
	// fieldPosByName in a loop.
	var err error

	tail := len(*fieldStack)
	*fieldStack = append(*fieldStack, 0)
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		(*fieldStack)[tail] = i
		if sf.PkgPath != "" && !sf.Anonymous {
			// Field is unexported, skip it.
			continue
		}
		// Handle anonymous struct embedding, but do not try to handle embedded pointers.
		if sf.Anonymous && sf.Type.Kind() == reflect.Struct {
			err = rs.populateFields(fldDescs, sf.Type, fieldStack)
			if err != nil {
				return err
			}
		} else {
			dbTag, dbTagPresent := sf.Tag.Lookup(structTagKey)
			if dbTagPresent {
				dbTag, _, _ = strings.Cut(dbTag, ",")
			}
			if dbTag == "-" {
				// Field is ignored, skip it.
				continue
			}
			colName := dbTag
			if !dbTagPresent {
				colName = sf.Name
			}
			fpos := fieldPosByName(fldDescs, colName)
			if fpos == -1 {
				if rs.lax {
					continue
				}
				return fmt.Errorf("cannot find field %s in returned row", colName)
			}
			if fpos >= len(rs.fields) && !rs.lax {
				return fmt.Errorf("cannot find field %s in returned row", colName)
			}
			rs.fields[fpos] = structRowField{
				path: append([]int(nil), *fieldStack...),
			}
		}
	}
	*fieldStack = (*fieldStack)[:tail]

	return err
}

// structRowScannerInto encapsulates the logic to scan a row into fields of a struct.
type structRowScannerInto[T any] struct {
	fields      []structRowField
	scanTargets []any
}

func (rs *structRowScannerInto[T]) ScanRowInto(receiver *T, rows Rows) error {
	rs.setupScanTargets(receiver)
	return rows.Scan(rs.scanTargets...)
}

func (rs *structRowScannerInto[T]) setupScanTargets(receiver *T) {
	v := reflect.ValueOf(receiver).Elem()
	if rs.scanTargets == nil {
		rs.scanTargets = make([]any, len(rs.fields))
	}
	for i, f := range rs.fields {
		rs.scanTargets[i] = v.FieldByIndex(f.path).Addr().Interface()
	}
}

// addrScannerInfo wraps a RowScannerInto[T] into a RowScannerInto[*T].
type addrScannerInto[T any] struct {
	wrapped RowScannerInto[T]
}

// NewAddrScannerInto returns a RowScannerInto that wraps a RowScannerInto to scan into a pointer.
func NewAddrScannerInto[T any](wrapped RowScannerInto[T]) RowScannerInto[*T] {
	return &addrScannerInto[T]{wrapped: wrapped}
}

var _ RowScannerInto[*struct{}] = (*addrScannerInto[struct{}])(nil)

func (rs *addrScannerInto[T]) Initialize(rows Rows) error {
	return rs.wrapped.Initialize(rows)
}

func (rs *addrScannerInto[T]) ScanRowInto(receiver **T, rows Rows) error {
	*receiver = new(T)
	return rs.wrapped.ScanRowInto(*receiver, rows)
}

type mapScannerInto struct{}

var _ RowScannerInto[map[string]any] = (*mapScannerInto)(nil)

// NewMapScannerInto returns a RowScannerInto that scans a row into a map.
func NewMapScannerInto() RowScannerInto[map[string]any] {
	return &mapScannerInto{}
}

// RowIntoMap scans a row into a map.
func RowIntoMap() rowIntoSpecRes[map[string]any] {
	return rowIntoSpecRes[map[string]any]{fn: NewMapScannerInto}
}

func (*mapScannerInto) Initialize(rows Rows) error {
	return nil
}

func (*mapScannerInto) ScanRowInto(receiver *map[string]any, rows Rows) error {
	values, err := rows.Values()
	if err != nil {
		return err
	}

	*receiver = make(map[string]any, len(values))

	for i := range values {
		(*receiver)[rows.FieldDescriptions()[i].Name] = values[i]
	}

	return nil
}
