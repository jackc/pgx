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
pool settings distinguish defaults from explicit zero; nil versus empty
`ConnStringAllowedKeys` means unrestricted versus reject all string keys.
The allow-list only restricts the original string, including aliases and repeated
keys. Environment and service configuration are trusted inputs. Known unsupported
client settings and unknown keys are rejected rather than sent as server GUCs.

`pgx.Connect2` returns `*pgx.Conn2`; `pgxpool.New2` returns `*pgxpool.Pool2`.
These wrappers expose the shared query/lifecycle operations without `Config()`.
Inspect `conn.PgConn().ConnectionInfo()` for an independent selected-endpoint
snapshot. Keep the original string/options to reconnect. `LegacyConn()` and
`LegacyPool()` explicitly bridge to libraries that require the old concrete
types; their `Config()` returns nil for new-path objects. Existing query tracers,
transactions' `Conn()` methods, pool admission hooks, and stdlib raw connections
use that same compatibility view. They must not infer reconnect inputs from it.
Objects actually created by legacy APIs retain their original config getters.

### Factory composition

The public pgx factory constructor surrounds connection establishment with tracing
and initializes query caches and notification delivery:

```go
conn, err := pgx.ConnectFunc(ctx,
    func(ctx context.Context) (*pgconn.PgConn, error) {
        return pgconn.Connect2(ctx, connString, physicalOptions)
    },
    pgx.ConnOptions{},
)
```

The callback runs exactly once. It owns transport/host retries and transfers a
fresh, connected, idle `PgConn`. pgx closes any connection returned with an error,
or rejected by pgx initialization. A nil connection without an error is rejected.
The constructor's context, including tracing additions, reaches the callback.
This also works for third-party connection factories; adapters need no privileged
prepared configuration or parser handle.

The string/options wrapper is the ordinary entry point; it uses that same
factory constructor. Accepting an already established connection would lose the
ability to trace establishment and clean up a failed factory, so it is not a
separate constructor.

`pgconn.Connect2` buffers notifications without an explicit handler, including
those consumed by connection hooks. pgx installs its buffer and drains that queue
at handoff. An explicit `OnNotification` wins. Factories using legacy pgconn APIs
must arrange notification handling before connecting if setup consumes messages.
Low-level consumers can install a handler with `SetNotificationHandler` to drain
the queue. The connection requires exclusive ownership for handoff.

The recognized pgx string extensions are `statement_cache_capacity`,
`description_cache_capacity`, and `default_query_exec_mode`. They are validated
before dialing, retained as independent string values, and never sent to the
server. Explicit `ConnOptions` values override them. Defaults remain 512/512 and
`cache_statement`; zero capacities disable the corresponding caches.

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
application validation, low-level `AfterConnect`, pgx notification/cache setup,
pgx `AfterConnect`, then pool admission. Validation rejects the entire logical
host; wrapping/joining `pgconn.ErrAbortConnect` stops selection. Initialization
errors stop the operation. Rejected/admission-failed connections are closed.
Callbacks receive contexts and must cooperate with cancellation.

Pool creation uses a private factory for every resource, including initial fill
and replacement. The legacy factory retains config-copying and `BeforeConnect`.
The new factory retains only the original inputs. Admission, destruction, and
resource accounting stay in the pool. The new pool's outer connection timeout
defaults to two minutes; explicit zero disables it. It is separate from libpq's
per-address timeout. Pool sizes and lifecycle settings are supplied in `Options`,
not `pool_*` string extensions.

Standalone stdlib connectors use the same private factory pattern. Pool-backed
connectors acquire/release through the pool; `OpenDBFromPool2` sets database/sql's
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
The caller's context limits the entire operation. Address and host shuffling use
one operation-local random generator. Cancellation uses the selected address,
including Unix socket fallbacks, and preserves TLS and variable-length keys.
Hijack/Construct carry opaque new-path runtime and destination state.

## Capability matrix

| Capability | Status / boundary |
| --- | --- |
| URI and keyword syntax, aliases, duplicate keys, allowed keys | Shared lexical parser; stricter new-path client-key validation |
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

The capability matrix is intentionally narrower than libpq. Do not remove legacy
APIs or automatically migrate arbitrary Config mutations. Additional options
need independent implementations and tests.

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
| TraceConnectStart with ConnConfig | Implement pgx.ConnectTracer2; query/batch/etc. tracer interfaces remain shared |
| NewWithConfig / stdlib.OpenDB | New2 / OpenDB2 with separate lifecycle/options values |

Lints should first flag config-getter round trips, mutation of TLS/fallback fields,
and custom connect tracers. Renaming calls mechanically does not preserve retry
semantics or default-refresh behavior. Legacy parsing/config APIs remain available
for workloads needing capabilities that the new path does not yet cover.

## Validation and optional oracle

Normal Go protocol fixtures use `internal/pgmock.Connections`, controlled DNS,
and deterministic ordering. They require no database, C compiler, downloads, or
libpq. Run them with `go test ./pgconn -run '^TestConnect2'` (the optional oracle
skips without its environment variable). Integration tests use the existing
`PGX_TEST_*` variables and `DEVELOPMENT.md` lifecycle.

The optional C helper requires libpq 18.6 and rejects other linked versions:

```sh
cc -I"$(pg_config --includedir)" pgconn/testdata/connect2_oracle.c \
  -L"$(pg_config --libdir)" -Wl,-rpath,"$(pg_config --libdir)" -lpq \
  -o /tmp/pgx-connect2-oracle
PGX_TEST_LIBPQ_CONNECT2=/tmp/pgx-connect2-oracle \
  go test ./pgconn -run '^TestConnect2LibpqOracle$' -v
```

The manually dispatched `connect2-oracle.yml` workflow builds the exact source
commit above with OpenSSL and without GSS, and prints its configure settings.
The local comparison used Ubuntu's libpq 18.6, built with OpenSSL/GSS/LDAP; no GSS
credentials were supplied. Six numeric-address fixtures compare connection
success/failure and socket counts for the transition table's central cases.
They establish no differential DNS claim. Baseline changes require rerunning and
reviewing these comparisons; a newer server version alone does not update it.
