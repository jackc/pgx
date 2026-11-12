# Plan for a libpq compatible connection API

Date: 2026-11-12. Repository baseline: `ea996359`.

Build a separate connection path centered on PostgreSQL connection strings and Go options for
behavior that cannot be expressed appropriately in a string. Keep parsed connection state
internal. Expose an immutable, opaque handle only if a concrete application use case requires
one. Preserve the existing connection APIs until callers can migrate to their replacements.

This plan covers architecture, implementation, and migration. Names such as `Connect2` are
placeholders. The selected direction is a separate path with opaque state, not a second
publicly editable configuration system. Implementation belongs in a later session.

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

The minimum low-level interface is:

```go
func Connect2(ctx context.Context, connString string) (*PgConn, error)
func ConnectWithOptions2(ctx context.Context, connString string, opts ConnectOptions2) (*PgConn, error)
```

Provide corresponding pgx entry points and pool constructors. The established-connection
return types shown here are provisional until the accessor integration described below is
resolved. Keep ordinary query operations shared wherever practical.

`Connect2` is the default-options form of `ConnectWithOptions2`. Connection strings, environment
variables, and service files supply standard PostgreSQL settings: hosts, addresses, ports,
credentials, transport policy, session selection, and other supported libpq parameters.
Do not duplicate these as an editable `Config2` or a set of post-parse setters.

`ConnectOptions2` supplies Go-specific capabilities: custom dialing and lookup, TLS material
or customization, dynamic credential/token and SSL key-password callbacks, tracing, connection
hooks, frontend/context-watcher construction, message limits, and notice/notification/error
handlers. Preserve pgx query modes and cache settings through their existing string options
where available; put any necessary Go-only controls in the options API. Specify conflicts
between an explicit string policy and a customization rather than silently overriding policy.

An options struct may have exported fields: it is input, not mutable parsed state. Capture
its values on construction and copy or otherwise isolate referenced mutable data, including
slices, maps, and TLS objects. Later edits must not change a pool or prepared connection plan.
Callback closures and supplied services retain their documented synchronization obligations.
Do not provide a `Configure(func(*internalConfig))` escape hatch or equivalent setter surface.

### Optional opaque handle

Public parsing is not required initially. Pools and stdlib connectors can parse once and
reuse a private immutable connection plan. The plan can cross pgx package boundaries through
an internal implementation without becoming an application configuration API.

If applications need validation without connecting or reusable preparation outside those
adapters, consider a parse/prepare function returning an opaque handle. It would expose no
fields, mutation methods, or mutable references to its contents. Each connect operation
would allocate its own attempt state, making the handle safely reusable. Changing connection
settings would require preparing new inputs, not editing the handle. Do not add this interface
merely to mirror the legacy parser family.

The earlier proposal for application defaults/overrides modeled on `PQconnectdbParams` is
optional, not part of the minimum API. If added, merge them before normalization, define
ordering and empty-value semantics, and keep them as input parameters. They must not become
another way to mutate a prepared plan.

## Parsing and internal representation

Use this pipeline for the new entry points:

1. **Parse and normalize inputs.** Share lexical parsers where semantics agree, then resolve
   connection-string, environment, service-file, and default precedence. Preserve absent
   versus explicitly supplied values internally, especially for SSL defaulting. Validate
   host/address/port lists and unsupported combinations without resolving DNS.
2. **Capture an immutable connection plan.** Retain logical hosts, policies, credential and
   TLS sources, and captured Go options. Keep this representation independent of legacy
   `Config`; do not construct one and reverse its flattened output. Snapshot environment
   and service inputs when the plan is built, including for pools and stdlib connectors.
3. **Create per-operation state.** Resolve destinations lazily, select credentials, build TLS
   state, and track retries/deadlines without changing the plan. Specify when referenced
   credential files are loaded or refreshed so pooled reconnects behave predictably.
4. **Retain established-connection state.** Store the selected endpoint, negotiated transport,
   cancellation data, and runtime callbacks/settings needed after connection succeeds.

Internally, represent hosts as one ordered collection of logical name or socket location,
optional numeric `hostaddr`, and port. Keep names separate from network addresses. Policies
shared across hosts include SSL mode/negotiation, target-session attributes, host balancing, authentication
requirements, and timeouts. Resolved addresses, chosen passwords, server properties, and
transport attempts belong to operation-local state.

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
| API ownership | Options isolation, concurrent reuse of private/opaque plans, no mutable plan state exposed through hooks or diagnostics, and unchanged legacy edits/copies. |
| Integration | pgx execution settings, tracing/errors, pool creation/replenishment, stdlib reconnect/reset, hijack/reconstruction, and coexistence of old and new paths. |

Implement a deterministic local protocol simulator recording sockets, negotiation, startup,
authentication, validation queries, and cleanup. Use a controlled resolver and seeded ordering
in Go tests. Differential DNS claims need an equivalent controlled libpq setup; injecting only
pgx's resolver does not establish libpq behavior.

Add an optional pinned libpq client helper and differential job. Normal Go tests must not
require libpq, a C toolchain, external services, or network downloads. Convert confirmed
comparisons into regular regression tests. Existing parser oracle tests in
`pgconn/parse_url_test.go` and `pgconn/parse_keyword_value_test.go` provide useful precedent.
Also run relevant real-server TLS/authentication tests, supported server/platform checks, and
race tests for pools and reused plans through the lifecycle documented in `DEVELOPMENT.md`.

## Integration boundaries

Start the source audit in `pgconn/config.go`, `pgconn/pgconn.go`, `pgconn/auth_*`,
`pgconn/krb5.go`, and `pgconn/errors.go`, then follow callers through `conn.go`, `tracer.go`,
`pgxpool/pool.go`, and `stdlib/sql.go`. Resolve these boundaries in an API prototype:

| Boundary | Direction |
| --- | --- |
| `Conn.Config()` and `Pool.Config()` | Preserve legacy contracts. The new path does not require a config getter or editable round trip. If inspection is needed, expose limited immutable values or independent diagnostic snapshots, not internal state or a misleading legacy projection. |
| Established connection types | Prefer sharing `PgConn`, `Conn`, and `Pool`. If old config methods would acquire misleading or unexpectedly nil results, use a wrapper sharing implementation. Do not duplicate query machinery. |
| Errors and connection tracing | New failures/events expose useful destination, phase, and wrapped cause without requiring a legacy config or leaking secrets. Preserve context error and server error discovery through `errors.Is`/`errors.As`. |
| Pool construction and hooks | Accept the string and captured Go options, retain a private plan, and allocate state per physical connection. Pool sizing/lifecycle options remain separate. Replace configuration-editing hooks with specific providers or explicit new inputs where a demonstrated use case needs them. |
| stdlib | Add a typed connector accepting the new inputs. Keep `sql.Open("pgx", dsn)` and old registration tokens on the legacy path; any new DSN-only driver selection must be explicit. Pool-backed connections use their pool's path. |
| Hijack and reconstruction | Preserve destination, TLS, cancellation data, callbacks, and connection origin without rebuilding a new connection through legacy defaults. |

Retain pool timeout defaults as a documented outer pgx policy, distinct from libpq attempt
timeouts. Pool and stdlib initialization can parse privately without requiring public parse or
copy APIs. If a consumer needs different connection parameters for each physical connection,
use a narrowly defined source of complete new inputs and rerun normalization; do not hand it
the internal plan to edit. Introduce such a provider only for an identified migration need.

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
| 1. API prototype | Compilable pgconn/pgx/pool/stdlib examples using strings and Go options. Resolve accessor/wrapper behavior, option ownership, TLS/provider precedence, hook rejection, and credential refresh. Keep parsing private unless a concrete need justifies an opaque public handle. |
| 2. Behavioral fixtures | Permanent simulator, optional pinned client, phase-aware transition table, and legacy regression cases. Reproduce the earlier audit observations before refactoring. |
| 3. Input pipeline | Internal parsing/normalization and immutable plan, option capture, policy validation, and credential/TLS sources. No dependency on a flattened legacy configuration. |
| 4. Connection engine | Shared primitives with legacy tests passing, new scheduler and constructors, selected-destination state, cleanup, and passing core behavior comparisons. |
| 5. Integration | Working pgx, pool, stdlib, tracing, errors, and hijack/reconstruction. Reconnects preserve the chosen path without public configuration manipulation. |
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
silent fallbacks. This branch currently contains the plan only.
