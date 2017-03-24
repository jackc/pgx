erb range_type=Int4range element_type=Int4 typed_range.go.erb > int4range.go
erb range_type=Int8range element_type=Int8 typed_range.go.erb > int8range.go
goimports -w *range.go
