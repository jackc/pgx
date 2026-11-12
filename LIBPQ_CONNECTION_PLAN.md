# Plan for a libpq compatible connection API

Date: 2026-11-12. Repository baseline: `ea996359`.

Build a separate connection path with one function accepting a PostgreSQL connection string
and Go options. Run input resolution and setup anew for each new physical connection. Keep
parsed state private to that connection operation; no reusable parsed handle is required.
Preserve the existing connection APIs until callers can migrate to their replacements.

This plan covers architecture, implementation, and migration. Names such as `Connect2` are
placeholders. The selected direction is a separate path with private state, not a second
publicly editable configuration system. The implementation status and resolved API choices are recorded below.

## Decision and legacy compatibility

Today, parsing flattens logical hosts and TLS alternatives into the primary configuration and
`Fallbacks`. Applications can then change individual fields independently of the assumptions
that produced them. Those edits make it impossible to reliably distinguish parser-generated
alternatives from deliberate application retry sequences.

The new API should accept inputs and own their interpretation. This keeps defaulting, host
selection, TLS, and credentials internally consistent without freezing the internal model as
a public contract. For humans and AI agents, explicit entry points and fewer mutable states
are preferable to hidden compatibility detection; parallel function names are acceptable.

- Keep existing exported signatures and types, including `Config` and `FallbackConfig`.
  Existing direct connection, parsing, configuration-based connection, pool, and stdlib entry
  points retain their legacy behavior and remain usable alongside the new path.
- Preserve the meaning of legacy field edits and `Copy()`. Clearing `Fallbacks` must not cause
  hidden state to restore an attempt. Do not infer an engine from edits, pointer identity,
  callbacks, or a process-wide mode switch.
- Share transport, authentication, protocol, and query machinery where behavior agrees.
  Keep scheduler differences explicit. Security or correctness fixes that also affect legacy
  callers need focused tests and release notes.
- Deprecate superseded connection APIs when replacements cover their use cases. Removal is
  a separate major-version decision.

## Public interfaces and ownership

Use one low-level connection function, with options always present:

```go
func Connect2(ctx context.Context, connString string, opts ConnectOptions2) (*PgConn, error)

// The zero options value requests the standard behavior.
conn, err := Connect2(ctx, connString, ConnectOptions2{})
```

Provide corresponding pgx entry points and pool constructors. The established-connection
return types shown here are provisional until the accessor integration described below is
resolved. Keep ordinary query operations shared wherever practical.

Do not add a second `WithOptions` convenience function. The working choice is an options
value; a pointer accepting `nil` is an alternative to settle during the API prototype, not a
second entry point. A pointer to the whole struct does not solve presence for its fields.

Connection strings, environment variables, and service files supply standard PostgreSQL
settings: hosts, addresses, ports, credentials, transport policy, session selection, and other
supported libpq parameters.
Do not duplicate these as an editable `Config2` or a set of post-parse setters.

`ConnectOptions2` supplies Go-specific capabilities: custom dialing and lookup, TLS material
or customization, dynamic credential/token and SSL key-password callbacks, tracing, connection
hooks, frontend/context-watcher construction, message limits, and notice/notification/error
handlers. Preserve pgx query modes and cache settings through their existing string options
where available; put any necessary Go-only controls in the options API. Specify conflicts
between an explicit string policy and a customization rather than silently overriding policy.

Specify presence per field. Use nil callbacks for default behavior where appropriate. For a
scalar whose zero is meaningful, distinguish unset from explicit zero/false with a pointer,
an optional value, or an explicit automatic enum value. Preserve nil versus empty collection
semantics where meaningful. No port override is planned; if one is ever introduced, an unset
override and an explicitly supplied zero must be distinguishable before validation.

An options struct may have exported fields: it is input, not mutable parsed state. Capture
its values on entry or adapter construction and isolate referenced mutable data, including
slices, maps, and supplied TLS objects. Later caller edits must not change retained inputs.
Callback closures and supplied services retain their documented synchronization obligations.
Do not provide a `Configure(func(*internalConfig))` escape hatch or equivalent setter surface.

### Connection lifetime and refresh

Direct callers, pools, stdlib connectors, and third-party connection factories use the same
path. Reusable consumers retain the original string and captured Go options, then invoke the
connection function for each physical connection they create or replace. They receive no
privileged parse-once API, cached resolved configuration, or prepared token. Existing live
connections are reused normally; a pool checkout is not a new physical connection.

Each new connection operation reparses the string and resolves current environment/service
settings. Read password and TLS files at the baseline-defined stages, and resolve hostnames
when reached. Do not cache their resolved contents across physical connections. Explicit
string values still override external defaults, and `hostaddr` still bypasses DNS.

One operation may try several hosts, addresses, or transports. Keep its normalized settings
stable across those retries rather than restarting parsing and defaulting before every socket.
Follow libpq's stage-specific DNS and credential-loading rules within that operation.

Fresh lookup means invoking the resolver, not bypassing OS or custom-resolver caches. File
refresh applies to configured paths; certificates or roots supplied as Go objects are captured
values, and a provider is needed to refresh application-managed material. Existing sessions
do not change when files rotate. Changes to environment or service files can affect subsequent
connections unless their values are explicitly supplied in the input string.

Syntax and option validation may run early in adapter constructors, but it must not freeze
external inputs or replace validation during connection. A reusable parse API is out of the
initial scope. Consider caching only after measuring a meaningful cost, while preserving this
refresh contract; lexical parsing and external resource resolution are different operations.

The earlier proposal for application defaults/overrides modeled on `PQconnectdbParams` is
optional, not part of the minimum API. If added, merge them before normalization, define
ordering and empty-value semantics, and keep them as input parameters. They must not become
mutators for internal settings.

## Parsing and internal representation

Use this pipeline for the new entry points:

1. **Parse and normalize inputs.** Share lexical parsers where semantics agree, then resolve
   connection-string, environment, service-file, and default precedence. Preserve absent
   versus explicitly supplied values internally, especially for SSL defaulting. Validate
   host/address/port lists and unsupported combinations without resolving DNS.
2. **Build private normalized settings for this operation.** Retain logical hosts, policies,
   credential/TLS sources, and captured Go options. Keep this representation independent of
   legacy `Config`; do not construct one and reverse its flattened output. Environment and
   service inputs are resolved again on the next connection operation.
3. **Create attempt state.** Resolve destinations lazily, select credentials, build TLS state,
   and track retries/deadlines separately from normalized settings. Retain only what this
   operation or the eventual established connection needs.
4. **Retain established-connection state.** Store the selected endpoint, negotiated transport,
   cancellation data, and runtime callbacks/settings needed after connection succeeds.

Internally, represent hosts as one ordered collection of logical name or socket location,
optional numeric `hostaddr`, and port. Keep names separate from network addresses. Policies
shared across hosts include SSL mode/negotiation, target-session attributes, host balancing,
authentication requirements, and timeouts. Resolved addresses, chosen passwords, server
properties, and transport attempts belong to operation-local state.

Recognize client parameters separately from server runtime parameters. Reject known unsupported
client options explicitly rather than forwarding them as startup GUCs. Prefer rejecting
unknown string keys except documented pgx extensions; use PostgreSQL's `options` parameter
for server settings. Preserve a Go runtime-parameter extension only where a concrete use case
requires it, and document its precedence. Keep the legacy parser's behavior unchanged.

Carry forward the connection-string key restriction option. Validate untrusted keys, including
repetitions and aliases, before service or credential file access. If application parameter
arrays are added, define their trust boundary separately.

## Connection engine and customization

The new scheduler keeps host index, address index, target-selection pass, protocol phase,
allowed/attempted transports, credentials, deadline, and accumulated errors. Give lower-level
operations explicit attempt inputs instead of having them read the primary legacy config.
Use phase-aware transitions: continue on the socket, reconnect the address with another
transport, advance address, advance host, restart the selection pass, accept, or stop.

Build TLS state per destination with the correct verification identity, SNI, and ALPN. Decide
in the API prototype whether Go customization uses supplied material, a callback over a fresh
TLS configuration, or both. Document whether verification callbacks replace or extend built-in
verification. They must not reenable a transport prohibited by the connection-string policy.
Custom transports and validation are explicit pgx extensions to the default libpq behavior.

Keep target-session selection separate from application validation. Proposed hook order is
transport setup, authentication/startup, built-in target check, application validation, then
`AfterConnect` for the selected connection. Define application rejection as moving to the next
logical host, and an explicit abort result as ending the operation. Hooks receive the
connection when available and necessary immutable attempt information, not editable parsed state.
Dynamic credentials belong in dedicated provider hooks rather than pool callbacks that rewrite
configuration; define their precedence against string passwords and password files.

Timeout scopes follow host/address positions, not formatted address equality. The caller's
context can shorten the entire operation. Pass it to callbacks and require cooperation with
cancellation; Go cannot forcibly interrupt arbitrary callback code. Close rejected sockets,
release timers promptly, and avoid detached attempts or callback goroutines that outlive the
operation.

Authentication and cancellation must use the selected destination, including fallback Unix
sockets, rather than the first configured host. Preserve existing encrypted cancellation and
protocol-dependent key handling. Decouple post-connect query settings, caches, handlers, and
context watchers from the public legacy configuration.

## Behavior baseline and validation

Target libpq **18.6**, commit `724edf9bde9d356724ad384a2e196edc3c9f80f7`, initially. Record the
reference client's build features; its version is separate from supported server versions.
Baseline updates require explicit comparison. Use the [connection documentation](https://www.postgresql.org/docs/18/libpq-connect.html)
and [pinned connection source](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/interfaces/libpq/fe-connect.c)
to derive transitions, then verify them against the client. Error handling depends on protocol
phase as well as SQLSTATE.

The matrix combines earlier audit observations with required regression coverage. Reproduce
observations in permanent tests and recheck for independent fixes; temporary audit files and
this conversation are not test dependencies.

| Area | Required behavior and cases |
| --- | --- |
| Hosts and addresses | Resolve only when reached; preserve host grouping; support explicit addresses, socket paths, failed lookup, and repeated equal addresses at distinct positions. Confirm resolution lifetime across preference passes. |
| Transport retries | Apply alternatives to the current address. Under `prefer`, an allowed SSLRequest `N` continues plaintext on the same socket. Distinguish refusal, handshake failure, and startup failure for each supported SSL mode. |
| Error transitions | Match phase-specific handling of `57P03`, `53300`, and `28P01`. Earlier comparisons found libpq stopped on plaintext-only startup `53300`, but could retry TLS after plaintext `28P01` under `allow`. |
| Target selection | Use startup status when available, query only when needed, skip the current host on mismatch, and perform the complete relaxed second pass for `prefer-standby`. Revalidate application criteria on that pass. |
| Host balancing | Implement both host ordering and address ordering within a host, using operation-local randomness. |
| Credentials | Match password files per effective host/port/user/database, including default database and permission checks. Test explicit password/provider precedence and credential refresh. |
| TLS and authentication | Enforce channel binding across trust/password/SCRAM paths, direct-TLS ALPN, SNI independently of verification, trust-source/default rules, `require_auth`, and protocol negotiation validation. |
| Input handling | URL/keyword syntax, duplicate/empty values, default/environment/service precedence, negative timeout semantics, invalid SSL settings, allowed keys, and unsupported options. |
| Lifetimes | Per-address deadlines shared by transport retries, caller cancellation, cooperative hooks, resource cleanup, and cancellation against the selected endpoint. |
| Refresh and ownership | Default versus explicit zero/false and nil/empty options, isolation of retained inputs, concurrent connects, no exposed parsed state, and unchanged legacy edits/copies. Between connections change resolver answers, service/environment settings, password files, and TLS certificate/key/root files; check refresh through direct, pool, and stdlib paths. |
| Integration | pgx execution settings, tracing/errors, pool creation/replenishment through the selected factory, factory concurrency/context propagation, hook ordering and failure cleanup, stdlib reconnect/reset, hijack/reconstruction, and coexistence of old and new paths. |

Implement a deterministic local protocol simulator recording sockets, negotiation, startup,
authentication, validation queries, and cleanup. Use a controlled resolver and seeded ordering
in Go tests. Differential DNS claims need an equivalent controlled libpq setup; injecting only
pgx's resolver does not establish libpq behavior.

Add an optional pinned libpq client helper and differential job. Normal Go tests must not
require libpq, a C toolchain, external services, or network downloads. Convert confirmed
comparisons into regular regression tests. Existing parser oracle tests in
`pgconn/parse_url_test.go` and `pgconn/parse_keyword_value_test.go` provide useful precedent.
Also run relevant real-server TLS/authentication tests, supported server/platform checks, and
race tests for pools and retained inputs through the lifecycle documented in `DEVELOPMENT.md`.

### Connection lifecycle precedents

Source inspection supports creating connections directly from inputs, without requiring an
application-visible parsed object. These implementations are architectural precedents; libpq
remains the behavior oracle.

| Implementation inspected | Relevant behavior |
| --- | --- |
| libpq 18.6 | A new `PQconnectdb`/`PQconnectStart` parses and derives options, including per-host password-file entries. TLS initialization creates a new context and loads configured files. `PQconninfoParse` is optional inspection, not a required prepared-connection object. `PQreset` differs: it retains derived options/passwords while restarting connection setup and DNS. See the [constructor and reset source](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/interfaces/libpq/fe-connect.c) and [TLS initialization](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/interfaces/libpq/fe-secure-openssl.c#L771). |
| Ruby pg 1.6.3 | Ordinary connection creation parses arguments/defaults and resolves names before calling libpq. Its default reset path retains resolved options and renews DNS, then creates a new underlying libpq connection. It eagerly resolves hosts, so its scheduler is not an exact libpq model. See [Ruby connection setup](https://github.com/ged/ruby-pg/blob/v1.6.3/lib/pg/connection.rb#L891) and [reset implementation](https://github.com/ged/ruby-pg/blob/v1.6.3/ext/pg_connection.c#L568). |
| pgJDBC 42.7.8 | `Driver.connect` parses the URL for each physical connection, including calls from `BaseDataSource`; its default TLS factory creates new TLS state and loads configured files as needed. Some defaults and [JVM DNS results](https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/net/InetAddress.html) are cached. See [driver](https://github.com/pgjdbc/pgjdbc/blob/REL42.7.8/pgjdbc/src/main/java/org/postgresql/Driver.java#L246), [data source](https://github.com/pgjdbc/pgjdbc/blob/REL42.7.8/pgjdbc/src/main/java/org/postgresql/ds/common/BaseDataSource.java#L101), and [TLS factory](https://github.com/pgjdbc/pgjdbc/blob/REL42.7.8/pgjdbc/src/main/java/org/postgresql/ssl/LibPQFactory.java#L101). |

## Integration boundaries

Start the source audit in `pgconn/config.go`, `pgconn/pgconn.go`, `pgconn/auth_*`,
`pgconn/krb5.go`, and `pgconn/errors.go`, then follow callers through `conn.go`, `tracer.go`,
`pgxpool/pool.go`, and `stdlib/sql.go`. Resolve these boundaries in an API prototype:

| Boundary | Direction |
| --- | --- |
| `Conn.Config()` and `Pool.Config()` | Preserve legacy contracts. The new path does not require a config getter or editable round trip. If inspection is needed, expose limited immutable values or independent diagnostic snapshots, not internal state or a misleading legacy projection. |
| Established connection types | Prefer sharing `PgConn`, `Conn`, and `Pool`. If old config methods would acquire misleading or unexpectedly nil results, use a wrapper sharing implementation. Do not duplicate query machinery. |
| Errors and connection tracing | New failures/events expose useful destination, phase, and wrapped cause without requiring a legacy config or leaking secrets. Preserve context error and server error discovery through `errors.Is`/`errors.As`. |
| Pool construction and hooks | Select a private connection factory at construction, as described below. Pool sizing/lifecycle options remain separate. Replace configuration-editing hooks with specific providers or explicit new inputs where a demonstrated use case needs them. |
| stdlib | Add a typed connector accepting the new inputs. Keep `sql.Open("pgx", dsn)` and old registration tokens on the legacy path; any new DSN-only driver selection must be explicit. Pool-backed connections use their pool's path. |
| Hijack and reconstruction | Preserve destination, TLS, cancellation data, callbacks, and connection origin without rebuilding a new connection through legacy defaults. |

Retain pool timeout defaults as a documented outer pgx policy, distinct from libpq attempt
timeouts. If a consumer needs different connection parameters for each physical connection,
use a narrowly defined source of complete new inputs; do not hand it internal settings to
edit. Introduce such a provider only for an identified migration need, and make the same
capability available to third-party consumers rather than reserving it for built-in adapters.

### Internal connection factories

Give the pool a private connection function selected by its constructor. Using the existing
established-connection type for illustration:

```go
type connectFunc func(context.Context) (*pgx.Conn, error)
```

Every resource creation, including initial fill and replacement, calls this function. The
legacy factory preserves config copying, `BeforeConnect`, and timeout behavior; the new factory
captures the original string and isolated Go options and calls the ordinary new pgx entry point.
It retains inputs only, preserving the refresh contract above. This lets connection creation
evolve without coupling the pool lifecycle to a particular public connection API.

The factory must support concurrent calls, honor the supplied context, and return a fresh,
fully connected connection whose ownership transfers to the consumer. It cleans up failed
attempts. Pool admission hooks, resource accounting, lifetime limits, and destruction remain
outside it; the pool closes a connection if admission fails. Assign each hook to one layer so
connection hooks and pool hooks retain their documented order without duplicate invocation
by the adapter.

Use the same private pattern for standalone stdlib connectors. Pool-backed stdlib connectors
continue to acquire and release through their pool, preserving ownership and avoiding a second
creation path. Keep these functions local to their consumers; no shared factory framework,
exported callback option, or mutable global is needed initially. Constructor selection can
evolve later without changing the pool interface; exposing application-supplied factories or
switching a running pool's factory would require a separate public contract.

### pgx constructor alternative

Prototype the same separation between pgx and pgconn, potentially as a public constructor:

```go
func ConnectFunc(
    ctx context.Context,
    connect func(context.Context) (*pgconn.PgConn, error),
    opts ConnOptions,
) (*Conn, error)
```

Here `ConnOptions` contains pgx execution settings, tracing, and pgx-level initialization.
The callback owns connection establishment and receives the constructor's context, including
any tracing additions. Invoke it once; pgconn owns host/transport retries. Apply the ownership
and cleanup rules above to the handoff. Changing the underlying connection entry point or
its inputs would then require changing only the callback, while pgx-specific options and the
established-connection contract remain explicit API dependencies.

This is more than replacing a call to `pgconn.ConnectConfig`. Today pgx installs notification
buffering before connecting and extracts query/cache settings from the connection string.
An opaque factory cannot provide those integration points by itself. Resolve notification
delivery across setup and handoff, including messages consumed by connection hooks; define how
pgx string extensions reach pgx without being rejected or sent as server parameters; and
replace config-dependent tracing/accessors as already required by the new path. Moving pgx
settings exclusively into options is an alternative, but changes the planned string support
and needs an explicit migration decision.

Compare a factory-based primary constructor with a direct string/options constructor backed
by a private factory. A thin string/options wrapper can coexist with the public factory if
ordinary usage warrants it. A constructor accepting an already established `*pgconn.PgConn`
is another composition option, but cannot surround establishment with pgx tracing or own
cleanup during that step. Settle this choice in the API prototype rather than promising that
any signature will never need to evolve.

## Scope beyond the scheduler

The initial release must cover the matrix above, including `hostaddr`, `load_balance_hosts`,
per-host passwords, and existing pgx transport/authentication capabilities: direct TLS, SCRAM,
channel binding, required-authentication policy, token-provider OAuth, protocol limits, and
cancellation. Fix the identified TLS/authentication enforcement gaps independently of retry
policy so the new path does not inherit them.

Maintain a capability matrix for additional libpq options. Implement tractable additions in
separate changes and mark the rest explicitly unsupported or as documented pgx differences:

- Environment/default handling, `fallback_application_name`, `client_encoding=auto`, service-file
  search and `PGSYSCONFDIR`, and platform default locations.
- Keepalives, `tcp_user_timeout`, abstract sockets, and `requirepeer`.
- TLS certificate/key formats and permissions, trust roots, CRLs, `sslcertmode`, protocol
  limits, key logging, and certificate-name semantics.
- GSS encryption/delegation, SSPI, SCRAM key options, built-in OAuth discovery/device flow,
  LDAP service lookup, and OpenSSL engines/configuration.

The last group contains independent projects rather than prerequisites for the scheduler.
Avoid claiming full libpq parity before the capability matrix supports it. Consult the
[SSL documentation](https://www.postgresql.org/docs/18/libpq-ssl.html),
[pinned TLS implementation](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/interfaces/libpq/fe-secure-openssl.c),
[pinned authentication implementation](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/interfaces/libpq/fe-auth.c),
and the [password-file](https://www.postgresql.org/docs/18/libpq-pgpass.html),
[environment](https://www.postgresql.org/docs/18/libpq-envars.html),
[service-file](https://www.postgresql.org/docs/18/libpq-pgservice.html), and
[OAuth](https://www.postgresql.org/docs/18/libpq-oauth.html) documentation for these additions.

## Implementation sequence

Complete each phase in reviewable commits, recording resolved choices and capability status.

| Phase | Deliverable and exit condition |
| --- | --- |
| 1. API prototype | Compilable pgconn/pgx/pool/stdlib and third-party factory examples. Compare pgx's public factory constructor with the direct string/options form, resolving notification handoff and pgx string extensions. Resolve value versus pointer options, field presence, accessor/wrapper behavior, TLS/provider precedence, and hook rejection. No reusable parsed handle. |
| 2. Behavioral fixtures | Permanent simulator, optional pinned client, phase-aware transition table, and legacy regression cases. Reproduce the earlier audit observations before refactoring. |
| 3. Input pipeline | Per-connection parsing/normalization, option capture, policy validation, and fresh external-input handling. No dependency on a flattened legacy configuration or adapter-specific prepared state. |
| 4. Connection engine | Shared primitives with legacy tests passing, new scheduler and constructors, selected-destination state, cleanup, and passing core behavior comparisons. |
| 5. Integration | Working pgx, pool, stdlib, tracing, errors, and hijack/reconstruction. Private factories select the legacy or new path at consumer construction, with lifecycle and ownership tests. All physical connections on the new path use the same refresh contract; pool reuse and existing sessions remain distinct. |
| 6. Migration | Primary examples, migration guide, capability matrix, and deprecation annotations pointing to working replacements. |
| 7. Further parity | Independent implementations of remaining options and explicit reviews of baseline updates. |

The migration guide must translate intent: old edits may become connection-string settings,
Go options/providers, or replacement connection inputs. Clearing `Fallbacks`, selecting hosts,
and controlling transport alternatives are not equivalent operations. Only automate migrations
whose intent is unambiguous. Explain behavioral changes separately from renamed functions,
provide lint guidance where useful, and preserve legacy-focused regression examples.

The initial implementation is complete when the supported behavior matrix and integration
checks pass, old callers retain their contract, and the documented new interfaces work without
exposing mutable parsed state. Run appropriate real-server, race/platform, formatting, and
lint checks. Document extensions and unsupported capabilities rather than treating them as
silent fallbacks. Implementation and migration details are in `CONNECTION_STRATEGY.md`.


## Implementation record

The initial implementation uses value options, private normalized inputs, and a
shared protocol runtime independent of legacy `Config`. `pgconn.Connect2` is the
physical entry point. `pgx.ConnectFunc` is public, with `pgx.Connect2` as its thin
string/options wrapper. It invokes the factory once, owns handoff cleanup, and
transfers buffered setup notifications. pgx string extensions remain supported.

`Conn2` and `Pool2` wrap shared query/lifecycle implementations without promoting
legacy `Config` getters. Explicit compatibility bridges support old concrete-type
APIs; no legacy configuration is synthesized. Connect tracing has a separate
config-free interface; query tracing remains shared.

TLS customization is a material provider, with independent string-controlled
verification/SNI/ALPN and additive application verification. Password providers
override string/password-file credentials. Validation rejection advances a logical
host; `ErrAbortConnect` stops. Pool timeout policy is a separately captured outer
limit. Pool and standalone stdlib construction select private factories, and
pool-backed stdlib continues acquiring/releasing through its pool.

Permanent Go fixtures and an optional version-checked libpq helper cover phase
transitions. The manual oracle workflow pins the source baseline. Additional
libpq options remain explicitly unsupported according to the capability matrix.
Legacy Config-based APIs are not broadly deprecated while they still expose
customization that lacks a new-path equivalent; migration guidance identifies
those boundaries instead of promising a mechanical rename.
