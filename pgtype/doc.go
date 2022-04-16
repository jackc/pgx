// Package pgtype converts between Go and PostgreSQL values.
/*
The primary type is the Map type. It is a map of PostgreSQL types identified by OID (object ID) to a Codec. A Codec is
responsible for converting between Go and PostgreSQL values. NewMap creates a Map with all supported standard PostgreSQL
types already registered. Additional types can be registered with Map.RegisterType.

Use Map.Scan and Map.Encode to decode PostgreSQL values to Go and encode Go values to PostgreSQL respectively.

JSON Support

pgtype automatically marshals and unmarshals data from json and jsonb PostgreSQL types.

Array Support

ArrayCodec implements support for arrays. If pgtype supports type T then it can easily support []T by registering an
ArrayCodec for the appropriate PostgreSQL OID. In addition, Array[T] type can support multi-dimensional arrays.

Composite Support

CompositeCodec implements support for PostgreSQL composite types. Go structs can be scanned into if the public fields of
the struct are in the exact order and type of the PostgreSQL type or by implementing CompositeIndexScanner and
CompositeIndexGetter.

Enum Support

PostgreSQL enums can usually be treated as text. However, EnumCodec implements support for interning strings which can reduce memory usage.

Array, Composite, and Enum Type Registration

Array, composite, and enum types can be easily registered from a pgx.Conn with the LoadType method.

Extending Existing Type Support

Generally, all Codecs will support interfaces that can be implemented to enable scanning and encoding. For example,
PointCodec can use any Go type that implements the PointScanner and PointValuer interfaces. So rather than use
pgtype.Point and application can directly use its own point type with pgtype as long as it implements those interfaces.

Sometimes pgx supports a PostgreSQL type such as numeric but the Go type is in an external package that does not have
pgx support such as github.com/shopspring/decimal. These types can be registered with pgtype with custom conversion
logic. See https://github.com/jackc/pgx-shopspring-decimal and https://github.com/jackc/pgx-gofrs-uuid for a example
integrations.

Entirely New Type Support

If the PostgreSQL type is not already supported then an OID / Codec mapping can be registered with Map.RegisterType.
There is no difference between a Codec defined and registered by the application and a Codec built in to pgtype. See any
of the Codecs in pgtype for Codec examples and for examples of type registration.

Encoding Unknown Types

pgtype works best when the OID of the PostgreSQL type is known. But in some cases such as using the simple protocol the
OID is unknown. In this case Map.RegisterDefaultPgType can be used to register an assumed OID for a particular Go type.

Overview of Scanning Implementation

The first step is to use the OID to lookup the correct Codec. If the OID is unavailable, Map will try to find the OID
from previous calls of Map.RegisterDefaultPgType. The Map will call the Codec's PlanScan method to get a plan for
scanning into the Go value. A Codec will support scanning into one or more Go types. Oftentime these Go types are
interfaces rather than explicit types. For example, PointCodec can use any Go type that implments the PointScanner and
PointValuer interfaces.

If a Go value is not supported directly by a Codec then Map will try wrapping it with additional logic and try again.
For example, Int8Codec does not support scanning into a renamed type (e.g. type myInt64 int64). But Map will detect that
myInt64 is a renamed type and create a plan that converts the value to the underlying int64 type and then passes that to
the Codec (see TryFindUnderlyingTypeScanPlan).

These plan wrappers are contained in Map.TryWrapScanPlanFuncs. By default these contain shared logic to handle renamed
types, pointers to pointers, slices, composite types, etc. Additional plan wrappers can be added to seamlessly integrate
types that do not support pgx directly. For example, the before mentioned
https://github.com/jackc/pgx-shopspring-decimal package detects decimal.Decimal values, wraps them in something
implementing NumericScanner and passes that to the Codec.

Map.Scan and Map.Encode are convenience methods that wrap Map.PlanScan and Map.PlanEncode.  Determining how to scan or
encode a particular type may be a time consuming operation. Hence the planning and execution steps of a conversion are
internally separated.
*/
package pgtype
