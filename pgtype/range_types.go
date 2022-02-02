// Do not edit. Generated from pgtype/range_types.go.erb
package pgtype

type Int4range struct {
	Lower     Int4
	Upper     Int4
	LowerType BoundType
	UpperType BoundType
	Valid     bool
}

func (r Int4range) IsNull() bool {
	return !r.Valid
}

func (r Int4range) BoundTypes() (lower, upper BoundType) {
	return r.LowerType, r.UpperType
}

func (r Int4range) Bounds() (lower, upper interface{}) {
	return &r.Lower, &r.Upper
}

func (r *Int4range) ScanNull() error {
	*r = Int4range{}
	return nil
}

func (r *Int4range) ScanBounds() (lowerTarget, upperTarget interface{}) {
	return &r.Lower, &r.Upper
}

func (r *Int4range) SetBoundTypes(lower, upper BoundType) error {
	r.LowerType = lower
	r.UpperType = upper
	r.Valid = true
	return nil
}

type Int8range struct {
	Lower     Int8
	Upper     Int8
	LowerType BoundType
	UpperType BoundType
	Valid     bool
}

func (r Int8range) IsNull() bool {
	return !r.Valid
}

func (r Int8range) BoundTypes() (lower, upper BoundType) {
	return r.LowerType, r.UpperType
}

func (r Int8range) Bounds() (lower, upper interface{}) {
	return &r.Lower, &r.Upper
}

func (r *Int8range) ScanNull() error {
	*r = Int8range{}
	return nil
}

func (r *Int8range) ScanBounds() (lowerTarget, upperTarget interface{}) {
	return &r.Lower, &r.Upper
}

func (r *Int8range) SetBoundTypes(lower, upper BoundType) error {
	r.LowerType = lower
	r.UpperType = upper
	r.Valid = true
	return nil
}

type Numrange struct {
	Lower     Numeric
	Upper     Numeric
	LowerType BoundType
	UpperType BoundType
	Valid     bool
}

func (r Numrange) IsNull() bool {
	return !r.Valid
}

func (r Numrange) BoundTypes() (lower, upper BoundType) {
	return r.LowerType, r.UpperType
}

func (r Numrange) Bounds() (lower, upper interface{}) {
	return &r.Lower, &r.Upper
}

func (r *Numrange) ScanNull() error {
	*r = Numrange{}
	return nil
}

func (r *Numrange) ScanBounds() (lowerTarget, upperTarget interface{}) {
	return &r.Lower, &r.Upper
}

func (r *Numrange) SetBoundTypes(lower, upper BoundType) error {
	r.LowerType = lower
	r.UpperType = upper
	r.Valid = true
	return nil
}

type Tsrange struct {
	Lower     Timestamp
	Upper     Timestamp
	LowerType BoundType
	UpperType BoundType
	Valid     bool
}

func (r Tsrange) IsNull() bool {
	return !r.Valid
}

func (r Tsrange) BoundTypes() (lower, upper BoundType) {
	return r.LowerType, r.UpperType
}

func (r Tsrange) Bounds() (lower, upper interface{}) {
	return &r.Lower, &r.Upper
}

func (r *Tsrange) ScanNull() error {
	*r = Tsrange{}
	return nil
}

func (r *Tsrange) ScanBounds() (lowerTarget, upperTarget interface{}) {
	return &r.Lower, &r.Upper
}

func (r *Tsrange) SetBoundTypes(lower, upper BoundType) error {
	r.LowerType = lower
	r.UpperType = upper
	r.Valid = true
	return nil
}

type Tstzrange struct {
	Lower     Timestamptz
	Upper     Timestamptz
	LowerType BoundType
	UpperType BoundType
	Valid     bool
}

func (r Tstzrange) IsNull() bool {
	return !r.Valid
}

func (r Tstzrange) BoundTypes() (lower, upper BoundType) {
	return r.LowerType, r.UpperType
}

func (r Tstzrange) Bounds() (lower, upper interface{}) {
	return &r.Lower, &r.Upper
}

func (r *Tstzrange) ScanNull() error {
	*r = Tstzrange{}
	return nil
}

func (r *Tstzrange) ScanBounds() (lowerTarget, upperTarget interface{}) {
	return &r.Lower, &r.Upper
}

func (r *Tstzrange) SetBoundTypes(lower, upper BoundType) error {
	r.LowerType = lower
	r.UpperType = upper
	r.Valid = true
	return nil
}

type Daterange struct {
	Lower     Date
	Upper     Date
	LowerType BoundType
	UpperType BoundType
	Valid     bool
}

func (r Daterange) IsNull() bool {
	return !r.Valid
}

func (r Daterange) BoundTypes() (lower, upper BoundType) {
	return r.LowerType, r.UpperType
}

func (r Daterange) Bounds() (lower, upper interface{}) {
	return &r.Lower, &r.Upper
}

func (r *Daterange) ScanNull() error {
	*r = Daterange{}
	return nil
}

func (r *Daterange) ScanBounds() (lowerTarget, upperTarget interface{}) {
	return &r.Lower, &r.Upper
}

func (r *Daterange) SetBoundTypes(lower, upper BoundType) error {
	r.LowerType = lower
	r.UpperType = upper
	r.Valid = true
	return nil
}
