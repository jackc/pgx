erb pgtype_array_type=Int2Array pgtype_element_type=Int2 element_type_name=int2 text_null=NULL binary_format=true typed_array.go.erb > int2_array.go
erb pgtype_array_type=Int4Array pgtype_element_type=Int4 element_type_name=int4 text_null=NULL binary_format=true typed_array.go.erb > int4_array.go
erb pgtype_array_type=Int8Array pgtype_element_type=Int8 element_type_name=int8 text_null=NULL binary_format=true typed_array.go.erb > int8_array.go
erb pgtype_array_type=BoolArray pgtype_element_type=Bool element_type_name=bool text_null=NULL binary_format=true typed_array.go.erb > bool_array.go
erb pgtype_array_type=DateArray pgtype_element_type=Date element_type_name=date text_null=NULL binary_format=true typed_array.go.erb > date_array.go
erb pgtype_array_type=TimestamptzArray pgtype_element_type=Timestamptz element_type_name=timestamptz text_null=NULL binary_format=true typed_array.go.erb > timestamptz_array.go
erb pgtype_array_type=TstzrangeArray pgtype_element_type=Tstzrange element_type_name=tstzrange text_null=NULL binary_format=true typed_array.go.erb > tstzrange_array.go
erb pgtype_array_type=TimestampArray pgtype_element_type=Timestamp element_type_name=timestamp text_null=NULL binary_format=true typed_array.go.erb > timestamp_array.go
erb pgtype_array_type=Float4Array pgtype_element_type=Float4 element_type_name=float4 text_null=NULL binary_format=true typed_array.go.erb > float4_array.go
erb pgtype_array_type=Float8Array pgtype_element_type=Float8 element_type_name=float8 text_null=NULL binary_format=true typed_array.go.erb > float8_array.go
erb pgtype_array_type=InetArray pgtype_element_type=Inet element_type_name=inet text_null=NULL binary_format=true typed_array.go.erb > inet_array.go
erb pgtype_array_type=MacaddrArray pgtype_element_type=Macaddr element_type_name=macaddr text_null=NULL binary_format=true typed_array.go.erb > macaddr_array.go
erb pgtype_array_type=CIDRArray pgtype_element_type=CIDR element_type_name=cidr text_null=NULL binary_format=true typed_array.go.erb > cidr_array.go
erb pgtype_array_type=TextArray pgtype_element_type=Text element_type_name=text text_null=NULL binary_format=true typed_array.go.erb > text_array.go
erb pgtype_array_type=VarcharArray pgtype_element_type=Varchar element_type_name=varchar text_null=NULL binary_format=true typed_array.go.erb > varchar_array.go
erb pgtype_array_type=BPCharArray pgtype_element_type=BPChar element_type_name=bpchar text_null=NULL binary_format=true typed_array.go.erb > bpchar_array.go
erb pgtype_array_type=ByteaArray pgtype_element_type=Bytea element_type_name=bytea text_null=NULL binary_format=true typed_array.go.erb > bytea_array.go
erb pgtype_array_type=ACLItemArray pgtype_element_type=ACLItem element_type_name=aclitem text_null=NULL binary_format=false typed_array.go.erb > aclitem_array.go
erb pgtype_array_type=HstoreArray pgtype_element_type=Hstore element_type_name=hstore text_null=NULL binary_format=true typed_array.go.erb > hstore_array.go
erb pgtype_array_type=NumericArray pgtype_element_type=Numeric element_type_name=numeric text_null=NULL binary_format=true typed_array.go.erb > numeric_array.go
erb pgtype_array_type=UUIDArray pgtype_element_type=UUID element_type_name=uuid text_null=NULL binary_format=true typed_array.go.erb > uuid_array.go
erb pgtype_array_type=JSONBArray pgtype_element_type=Text element_type_name=text text_null=NULL binary_format=true typed_array.go.erb > jsonb_array.go

# While the binary format is theoretically possible it is only practical to use the text format.
erb pgtype_array_type=EnumArray pgtype_element_type=GenericText text_null=NULL binary_format=false typed_array.go.erb > enum_array.go

goimports -w *_array.go
