package zeronull

import (
	"github.com/jackc/pgx/v5/pgtype"
)

// Register registers the zeronull types so they can be used in query exec modes that do not know the server OIDs.
func Register(m *pgtype.Map) {
	m.RegisterDefaultPgType(Float8(0), "float8")
	m.RegisterDefaultPgType(Int2(0), "int2")
	m.RegisterDefaultPgType(Int4(0), "int4")
	m.RegisterDefaultPgType(Int8(0), "int8")
	m.RegisterDefaultPgType(Text(""), "text")
	m.RegisterDefaultPgType(Timestamp{}, "timestamp")
	m.RegisterDefaultPgType(Timestamptz{}, "timestamptz")
	m.RegisterDefaultPgType(UUID{}, "uuid")
}
