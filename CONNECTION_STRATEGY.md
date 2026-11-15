# Connection strategy and migration

The new constructors retain inputs, resolve them afresh for each physical
connection, and keep parsed state private. They coexist with the legacy APIs.
The compatibility target is libpq **18.6**, PostgreSQL commit
`724edf9bde9d356724ad384a2e196edc3c9f80f7`; this is not a claim of complete libpq
feature parity.

## Entry points

```go
low, err := pgconn.Connect2(ctx, connString, pgconn.ConnectOptions2{})
conn, err := pgx.Connect2(ctx, connString, pgx.ConnectOptions2{})
pool, err := pgxpool.New2(ctx, connString, pgxpool.Options{})
db := stdlib.OpenDB2(connString, stdlib.Options2{})
```

All options are values. Nil callbacks request defaults. Pointer execution and
pool settings distinguish defaults from explicit zero. Pool `MaxConns` and
`HealthCheckPeriod` are value fields with zero selecting their defaults (the greater
of 4 or `runtime.NumCPU()`, and one minute respectively); negative values are rejected.
Nil versus empty `ConnStringAllowedKeys` means unrestricted versus reject all string keys.
The allow-list only restricts the original string, including aliases and repeated
keys. Environment and service configuration are trusted inputs. Known unsupported
client settings and unknown keys are rejected rather than sent as server GUCs.

`pgx.Connect2` returns `*pgx.Conn`; `pgxpool.New2` returns `*pgxpool.Pool`.
Both paths use the existing connection, pool, acquired-connection, and transaction
types. `Config()` returns an independent snapshot for either path.

For a `Connect2` connection, `Connect2String` and `Connect2Options` capture the
original arguments. `ConnString()` also returns that string. Cache capacities and
query mode report the effective execution settings. The embedded `pgconn.Config`
is empty: it does not describe the selected endpoint or resolved environment,
service, password, or TLS settings. Use `conn.PgConn().ConnectionInfo()` for the
selected endpoint. Its `TLS` field contains a `*tls.ConnectionState` snapshot,
or nil for plaintext or before a successful handshake. Provider callbacks receive
nil before TLS is established; later hooks, errors, and connection snapshots include
negotiated state where available. Nested TLS slices and certificates are shared
with crypto/tls and must be treated as read-only.

For a `New2` pool, `New2Options` captures all original pool and connection options,
including whether pointer settings were omitted or explicitly zero. The existing
lifecycle fields report effective pool settings. `ConnConfig` contains captured
connection inputs; execution settings are resolved per physical connection, so
inspect an acquired connection's config for those.

```go
config := conn.Config()
replacement, err := pgx.Connect2(ctx, config.ConnString(), *config.Connect2Options)

poolConfig := pool.Config()
replacementPool, err := pgxpool.New2(ctx, poolConfig.ConnString(), *poolConfig.New2Options)
```

Reusing these arguments repeats input resolution; it does not freeze the prior
connection's destination or external settings. Config snapshots and their `Copy`
methods copy pointer values and slices. Callbacks and tracer implementations
remain shared references. Editing a snapshot does not change existing objects
or the inputs used for future physical connections in a pool.

Passing these snapshots to `ConnectConfig` or `NewWithConfig` panics: those
constructors require configs created by their corresponding `ParseConfig` functions.
Use the original string and captured options with `Connect2` or `New2` instead.
Legacy configs retain their original semantics, and legacy constructors ignore
the added input fields.

`Connect2` uses the existing `ConnectTracer` interface. At start, `ConnConfig` is
an independent captured-input snapshot; its legacy connection fields are empty
and execution settings have not been resolved. Tracers must inspect the new input
fields or the final connection's `ConnectionInfo()` rather than assume legacy
fields describe the new path. The built-in logger reports destination information
without logging the captured connection string or credentials.

### Connection initialization

Notifications received during `pgx.Connect2` setup hooks remain available through
`WaitForNotification`, unless an explicit `OnNotification` handler is supplied.
Low-level `pgconn.Connect2` consumers must supply `OnNotification` to receive
notifications; pgconn does not buffer them without a handler.

The recognized pgx string extensions are `statement_cache_capacity`,
`description_cache_capacity`, and `default_query_exec_mode`. They are validated
before dialing and never sent to the server. Explicit `ConnOptions` values
override them. Defaults remain 512/512 and `cache_statement`; zero capacities
disable the corresponding caches.

### Refresh and ownership

Each physical connection resolves string > service > environment > defaults,
then normalizes once for that operation. A pool checkout or database/sql reuse
keeps the existing session. Replacement connections resolve environment/service
settings, password files, TLS files, and DNS again. DNS is invoked when each host
is reached, including the relaxed preference pass; OS/custom resolver caching
remains the resolver's responsibility. Password-file matches are captured for
all hosts at operation start; a password provider runs once per reached host
position and overrides both the string and password file.

Adapters copy slices and pointer-valued settings at construction. A provider is
a captured function, not frozen provider output: its closure and supplied services
must support concurrent use. TLS providers return exclusively owned material;
roots are cloned. TLS files and provider material are loaded for reached TLS
attempts. Existing connections retain their TLS/cancellation material when files
rotate. A provider can replace trust roots or client certificates, but cannot
weaken string-selected verification, change SNI policy, or enable a prohibited
transport. Additional verification extends built-in verification.

Hook order is network/TLS setup, authentication/startup, target selection,
application validation, low-level `AfterConnect`, pgx cache setup,
pgx `AfterConnect`, then pool admission. Validation rejects the entire logical
host; wrapping/joining `pgconn.ErrAbortConnect` stops selection. Initialization
errors stop the operation. Rejected/admission-failed connections are closed.
Callbacks receive contexts and must cooperate with cancellation.

The new pool's outer connection timeout defaults to two minutes; explicit zero
disables it. It is separate from libpq's per-address timeout. Pool sizes and
lifecycle settings are supplied in `Options`, not `pool_*` string extensions.

Use the existing `GetPoolConnector` or `OpenDBFromPool` with a `New2` pool.
These adapters acquire/release through the pool; `OpenDBFromPool` sets database/sql's
idle count to zero and does not take ownership of the pool.

## Scheduler transitions

| Phase/result | New-path action |
| --- | --- |
| DNS failure or no addresses | Advance logical host |
| Dial failure / address deadline | Advance address, resetting the deadline for that position |
| SSLRequest `N`, mode `prefer` | Continue plaintext on that same socket |
| TLS handshake failure | Reconnect this address with plaintext if policy allows; otherwise stop |
| Startup `57P03`, before AuthenticationOk | Advance logical host, skipping its other addresses/transports |
| Other startup server errors, including `53300` / `28P01` | Retry another permitted transport on this address; stop when exhausted |
| Error after AuthenticationOk | Stop |
| Target mismatch or application rejection | Close and advance logical host |
| `prefer-standby` exhaustion | Repeat the entire host list with relaxed target selection and application validation |
| Application abort / initialization error | Close and stop |

Transport alternatives share one address-position deadline. Repeated identical
addresses at distinct positions get distinct deadlines. Negative/zero
`connect_timeout` means unlimited; positive values below two seconds become two.
The caller's context limits the entire operation. Cancellation uses the selected address,
including Unix socket fallbacks, and preserves TLS and variable-length keys.
`Hijack`/`Construct` preserve the connection's runtime settings and destination information.

## Capability matrix

| Capability | Status / boundary |
| --- | --- |
| URI and keyword syntax, aliases, duplicate keys, allowed keys | Supported; stricter new-path client-key validation |
| Multiple hosts, hostaddr, ports, lazy DNS, socket paths | Supported; Go resolver port overrides are an extension |
| Abstract Unix sockets | Supported on platforms where Go's Unix dialer supports `@` addresses |
| `load_balance_hosts=random` | Both logical hosts and addresses are shuffled |
| Target-session attributes | Supported; use startup statuses before issuing fallback queries |
| Password files | Per effective host/port/user/defaulted database; insecure Unix permissions ignored |
| TLS modes, direct TLS/ALPN, SNI, channel binding | Supported; direct TLS requires a compatible server (PostgreSQL 17+) |
| TLS protocol limits, PEM files, encrypted legacy PEM key hook | Supported; hook receives the connection context |
| Application-managed roots/certificates | TLS provider; policy is enforced independently of material |
| Trust / password / MD5 / SCRAM / SCRAM-PLUS / required-auth policy | Supported; required channel binding rejects non-binding authentication |
| OAuth | Token-provider authentication; no built-in discovery/device flow |
| GSS authentication | Existing registered GSS provider; selected logical host is used |
| Protocol 3.0/3.2 negotiation and message limits | Supported; reject duplicate/invalid negotiation and unrecognized options |
| `fallback_application_name`, explicit client encoding, service files | Supported; `PGSYSCONFDIR` fallback when the user service file is absent |
| Arbitrary startup GUCs | Use standard `options='-c name=value'`; no mutable RuntimeParams map |
| Compiled-in libpq defaults / system service directory | Go/platform defaults; no inferred libpq build paths |
| `client_encoding=auto` | Explicitly unsupported; supply encoding |
| Socket keepalive parameters / tcp_user_timeout / requirepeer | Explicitly unsupported string controls; custom DialFunc is available |
| CRL, sslcertmode, sslcompression, key logging, engines | Explicitly unsupported |
| GSS encryption/delegation, SSPI, SCRAM key options, LDAP services | Explicitly unsupported / separate projects |
| OpenSSL certificate-name and key-format corner cases | Go crypto/x509 and crypto/tls semantics; not an OpenSSL emulation |

The capability matrix is intentionally narrower than libpq. Legacy APIs remain
available for applications needing customization the new constructors do not support.

## Translating legacy intent

| Legacy intent | New equivalent |
| --- | --- |
| Change Host/Port/User/Database | Change original connection-string inputs |
| Refresh password in BeforeConnect | PasswordProvider; avoid retaining resolved credentials |
| Refresh certificates in BeforeConnect | File paths or TLSProvider |
| Clear Fallbacks to pin one endpoint | Supply one host/address and an explicit transport policy; these are separate choices |
| Custom DialFunc/LookupFunc/handlers | Go options |
| Rewrite TLSConfig | TLSMaterial provider plus string verification/transport policy; arbitrary TLSConfig mutations are not automatically translated |
| Edit RuntimeParams | Standard `options` string or dedicated standard parameter |
| Parse once and ConnectConfig repeatedly | Retain string and captured options; call Connect2 for each physical connection |
| TraceConnectStart with ConnConfig | Reuse ConnectTracer; inspect Connect2String/Connect2Options for new-path inputs and ConnectionInfo for the selected endpoint |
| NewWithConfig / stdlib.OpenDB | New2 / OpenDB2 with separate lifecycle/options values |

Review configuration mutations and custom connect tracers when migrating.
Renaming calls mechanically does not preserve retry semantics or default-refresh
behavior.

## Validation and optional oracle

Normal Go protocol fixtures use `internal/pgmock.Connections`, controlled DNS,
and deterministic ordering. They require no database, C compiler, downloads, or
libpq. Run them with `go test ./pgconn -run '^TestConnect2'` (the optional oracle
skips without its environment variable). Integration tests use the existing
`PGX_TEST_*` variables and `DEVELOPMENT.md` lifecycle.

The C helper is an optional local developer tool for checking assumptions about
libpq behavior. It is not run automatically in CI. Local `go test ./...` and
`./test.sh` skip the comparison unless `PGX_TEST_LIBPQ_CONNECT2` points to a compiled
helper. It requires a C compiler and libpq 18.6 headers and libraries, and rejects
other linked versions:

```sh
cc -I"$(pg_config --includedir)" pgconn/testdata/connect2_oracle.c \
  -L"$(pg_config --libdir)" -Wl,-rpath,"$(pg_config --libdir)" -lpq \
  -o /tmp/pgx-connect2-oracle
PGX_TEST_LIBPQ_CONNECT2=/tmp/pgx-connect2-oracle \
  go test ./pgconn -run '^TestConnect2LibpqOracle$' -v -count=1
```

Six numeric-address fixtures compare connection success/failure and socket counts
for the transition table's central cases; they do not compare DNS behavior.
Baseline changes require rerunning and reviewing these comparisons; a newer
server version alone does not update the client baseline.
