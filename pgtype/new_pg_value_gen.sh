erb go_type=ACLItem skip_binary=true prefer_text_format=true new_pg_value.erb > zzz.aclitem.go
erb go_type=Bit new_pg_value.erb > zzz.bit.go
erb go_type=Bool new_pg_value.erb > zzz.bool.go
erb go_type=Box new_pg_value.erb > zzz.box.go
erb go_type=BPChar prefer_text_format=true new_pg_value.erb > zzz.bpchar.go
erb go_type=Bytea new_pg_value.erb > zzz.bytea.go
erb go_type=CID new_pg_value.erb > zzz.cid.go
erb go_type=CIDR new_pg_value.erb > zzz.cidr.go
erb go_type=Circle new_pg_value.erb > zzz.circle.go
erb go_type=Date new_pg_value.erb > zzz.date.go
erb go_type=Float4 new_pg_value.erb > zzz.float4.go
erb go_type=Float8 new_pg_value.erb > zzz.float8.go
erb go_type=GenericBinary skip_text=true new_pg_value.erb > zzz.generic_binary.go
erb go_type=GenericText skip_binary=true prefer_text_format=true new_pg_value.erb > zzz.generic_text.go
erb go_type=Hstore new_pg_value.erb > zzz.hstore.go
erb go_type=Inet new_pg_value.erb > zzz.inet.go
erb go_type=Int2 new_pg_value.erb > zzz.int2.go
erb go_type=Int4 new_pg_value.erb > zzz.int4.go
erb go_type=Int8 new_pg_value.erb > zzz.int8.go
erb go_type=Interval new_pg_value.erb > zzz.interval.go
erb go_type=JSON prefer_text_format=true new_pg_value.erb > zzz.json.go
erb go_type=JSONB prefer_text_format=true new_pg_value.erb > zzz.jsonb.go
erb go_type=Line new_pg_value.erb > zzz.line.go
erb go_type=Lseg new_pg_value.erb > zzz.lseg.go
erb go_type=Macaddr new_pg_value.erb > zzz.macadder.go
erb go_type=Name new_pg_value.erb > zzz.name.go
erb go_type=Numeric new_pg_value.erb > zzz.numeric.go
erb go_type=OIDValue new_pg_value.erb > zzz.oid_value.go
erb go_type=OID new_pg_value.erb > zzz.oid.go
erb go_type=Path new_pg_value.erb > zzz.path.go
erb go_type=pguint32 new_pg_value.erb > zzz.pguint32.go
erb go_type=Point new_pg_value.erb > zzz.point.go
erb go_type=Polygon new_pg_value.erb > zzz.polygon.go
erb go_type=QChar skip_text=true new_pg_value.erb > zzz.qchar.go
erb go_type=Text prefer_text_format=true new_pg_value.erb > zzz.text.go
erb go_type=TID new_pg_value.erb > zzz.tid.go
erb go_type=Time new_pg_value.erb > zzz.time.go
erb go_type=Timestamp new_pg_value.erb > zzz.timestamp.go
erb go_type=Timestamptz new_pg_value.erb > zzz.timestamptz.go
# erb go_type=Unknown new_pg_value.erb > zzz.unknown.go
erb go_type=UUID new_pg_value.erb > zzz.uuid.go
erb go_type=Varbit new_pg_value.erb > zzz.varbit.go
erb go_type=Varchar prefer_text_format=true new_pg_value.erb > zzz.varchar.go
erb go_type=XID new_pg_value.erb > zzz.xid.go
goimports -w zzz.*
