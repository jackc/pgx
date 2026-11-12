# Plan for a libpq compatible connection API

Date: 2026-11-12. Repository baseline: `ea996359`.

Build a separate, explicitly selected connection path with its own configuration types.
Preserve the existing connection APIs and their interpretation of mutable configuration,
then deprecate them when their replacements cover the supported use cases. Share established
connection and query machinery wherever the behavioral contracts agree.

This is an implementation plan for a later session. The separate API is the selected
architecture. Names such as `Connect2`, `ParseConfig2`, and `Config2` below are placeholders;
they are not a requirement to use numeric suffixes. The initial API prototype must settle
the remaining integration details identified below before implementing the new engine.

## Objectives and compatibility contract

The new path should follow a pinned libpq implementation for connection parameter handling,
logical host selection, address selection, transport negotiation, authentication, and target
session selection. It should support the Go customization that makes pgx useful, including
custom dialing and lookup, tracing, callbacks, and explicit runtime parameters.

The old path remains available for applications that depend on its behavior. In particular:

- Keep existing exported signatures and types, including `Config` and `FallbackConfig`.
- Keep existing `Connect`, `ConnectWithOptions`, `ParseConfig`, `ParseConfigWithOptions`, and
  `ConnectConfig` on the legacy path. Apply the same rule to pool and stdlib entry points.
- Preserve the meaning of edits to legacy host, port, TLS, password, validator, and fallback
  fields. Clearing `Fallbacks` must never let hidden parser state restore a fallback.
- Do not select an engine by detecting field mutations, comparing function pointers,
  inspecting TLS pointer identity, or guessing whether fallback entries came from parsing.
- Do not introduce a global mode switch or change behavior based on an unrelated package's
  initialization. Do not make a configuration change modes implicitly during `Copy()`.
- New functions accept new configuration types. An old configuration is not a valid argument
  to the new configuration-based constructor, and vice versa.
- Existing applications can keep using legacy behavior while migrating individual connection
  owners. Both paths must work in the same process.

Compatibility does not require retaining security defects indefinitely. Fixes to shared
authentication or TLS primitives need their own tests and release notes when they also affect
legacy callers. Keep those changes distinct from a wholesale change in retry policy.

## Why use a separate public API

The existing parser flattens hosts and TLS alternatives into a primary configuration followed
by `Fallbacks`. For two hosts with `sslmode=prefer`, the result resembles
`A/TLS, A/plaintext, B/TLS, B/plaintext`. Address expansion then loses the intended grouping of
logical host, resolved address, and transport alternatives. The same public fallback sequence
could also have been deliberately constructed by an application; its intent is unknowable.

A separate configuration can preserve that structure directly. It also makes migration
mechanically visible to human reviewers and AI agents. A type error is a stronger boundary
than documentation about a hidden mode. An ordinary refactor from direct connection to
parse-modify-connect must not switch behavior.

The following alternatives are superseded for this implementation:

| Alternative | Reason for choosing a separate API instead |
| --- | --- |
| Infer legacy behavior from configuration edits | Cannot infer intent or dependence on unchanged legacy behavior; copying and callbacks complicate detection. |
| Global default captured at parse time | Adds process-wide policy without resolving the conflicting field meanings. |
| New behavior in `Connect`, legacy behavior in `ConnectConfig` | Adding a tracer through parse-modify-connect could unexpectedly change retry semantics. |
| One expanded configuration with a mode flag | Exposes fields and combinations that only work in one mode, requiring runtime enforcement. |
| Replace the API only in a new major version | Remains a future consolidation option; it does not provide the selected incremental migration within the current major version. |

Accept the temporary cost of parallel public connection APIs. Avoid duplicating the query,
type, transaction, COPY, or protocol-message APIs merely to make the new path possible.

## Compatibility baseline and evidence

Use PostgreSQL libpq **18.6**, source commit
`724edf9bde9d356724ad384a2e196edc3c9f80f7`, as the initial behavioral baseline. Pin the
differential test client to that version and record its build features. The PostgreSQL server
version is a separate dimension: the driver must still work with the repository's supported
servers. Updating the client baseline later requires an explicit review of behavior changes.

Consult the [PostgreSQL 18 connection documentation](https://www.postgresql.org/docs/18/libpq-connect.html)
and the pinned [libpq connection implementation](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/interfaces/libpq/fe-connect.c).
Where the documentation is broad, derive transitions from the pinned source and confirm them
with a running client. Do not translate a general statement about authentication failure into
an unconditional stop rule without accounting for transport retries and protocol phase.

Earlier investigation compared pgx with libpq using local protocol simulators. The following
observations are regression targets to reproduce in permanent tests, not a substitute for
those tests. They must not depend on this chat or temporary audit files.

| Scenario | Earlier observed difference |
| --- | --- |
| Server answers `N` to SSLRequest under `prefer` | libpq continues on the same socket; pgx reconnects for plaintext. |
| A hostname has multiple addresses and optional TLS | pgx repeats lookup and expands transport before addresses. |
| Startup returns `53300` in a plaintext-only attempt | libpq stops; pgx can continue to another host. |
| Plaintext startup returns `28P01` under `allow` | libpq can retry TLS on that address; pgx stops. |
| Two primaries under `prefer-standby` | libpq performs a new pass beginning with the first host; pgx retries the last nonpreferred candidate. |
| Different password-file entries for successive hosts | pgx can reuse the first host's password. |
| `channel_binding=require` with trust or cleartext authentication | pgx's existing enforcement can be bypassed outside SCRAM. |
| Direct TLS without negotiated PostgreSQL ALPN | libpq rejects it; pgx can accept it. |
| `sslsni=0` with hostname verification | pgx can still send SNI. |
| Certificate trust sources and explicit SSL options | pgx can use system roots or rewrite explicit modes where libpq rejects the configuration. |
| Invalid negotiation settings, negative timeout, or protocol 3.1 negotiation | Validation differs. |
| Implicit database name or permissive password-file permissions | Password-file matching or acceptance differs. |

Recheck these against the implementation branch before changing code; some may have received
independent fixes. Preserve the useful legacy baselines even where the new behavior differs.

## Public API architecture

Provide a coherent family at each layer. Illustrative low-level signatures are:

```go
func Connect2(ctx context.Context, connString string) (*PgConn, error)
func ConnectWithOptions2(ctx context.Context, connString string, opts ParseOptions2) (*PgConn, error)
func ParseConfig2(connString string) (*Config2, error)
func ParseConfigWithOptions2(connString string, opts ParseOptions2) (*Config2, error)
func ConnectConfig2(ctx context.Context, config *Config2) (*PgConn, error)
```

The high-level pgx family uses a distinct `ConnConfig2` containing the new low-level
configuration plus existing pgx execution settings. Add corresponding pool construction and
parsing interfaces, including parser options, and an explicit stdlib connector path. All new
entry points lead to the same new engine. Direct connection and parse-then-connect must be
equivalent for the same inputs and hooks.

Do not alias the new configuration types to the old ones or embed the old public connection
configuration in them. Do not replace the existing `ConnectWithOptions` parameter type.
Existing connection callbacks may be reused when their signatures and meaning fit; callbacks
that expose legacy configuration need new typed counterparts.

Parsing still has value: applications and pools can configure once, inspect and modify the
result, copy it, and connect repeatedly. A separate opaque connector abstraction is not
required for the first implementation.

### Configuration model

Use one ordered slice containing all logical hosts. The new type has no primary-host alias
and no `Fallbacks` field. A host entry describes a destination, not one transport attempt.
The following is a structural sketch, not a final exported declaration:

```go
type HostConfig2 struct {
    Host     string
    HostAddr string
    Port     uint16
}

type Config2 struct {
    Hosts              []HostConfig2
    User               string
    Database           string
    Password           string
    Passfile           string
    SSLMode            SSLMode2
    SSLNegotiation     SSLNegotiation2
    TargetSessionAttrs TargetSessionAttrs2
    LoadBalanceHosts   LoadBalanceHosts2
    ConnectTimeout     time.Duration

    // TLS material and customization, authentication options, runtime
    // parameters, dialing and lookup, context handling, and callbacks.
}
```

Define these contracts before publishing the type:

- `Host` is the logical name or socket location. `HostAddr` is an optional numeric address;
  supplying it must not discard the logical name needed by other connection operations.
- Keep host/address/port list validation and empty-element semantics in the parser. An
  explicitly requested default destination is distinct from an accidentally empty host slice.
- A custom password applies to all hosts. Password-file matching uses the effective host,
  port, user, and database for the current attempt; it never overwrites the shared password.
- Transport policy is explicit. A missing TLS customization means use the configured TLS
  policy, rather than implicitly selecting plaintext.
- Preserve absent versus explicit settings where libpq defaulting requires the distinction.
  Use public optional or automatic values for editable settings; do not retain a hidden
  snapshot that can override later field edits. This matters for SSL defaults in particular.
- Store declared settings separately from computed TLS configurations, selected passwords,
  resolved addresses, and observed server properties. Rebuild dependent values from current
  configuration when connecting, so edits do not leave stale hostname or credential state.
- Provide `Copy()` with independent mutable slices, maps, and appropriate TLS clones. Function
  values and explicitly immutable resources may be shared under a documented ownership rule.
- Treat supplied configuration as read-only during connection. Each connection owns its
  attempt state. Pool callbacks receive a copy, and retries do not mutate the pool's template.
- Start with configurations created through the new parsers and return a descriptive error
  for invalid new configurations. Do not require struct-literal construction in the first
  release. Validate again after caller or pool-hook edits.

Port existing Go controls deliberately: dial and lookup functions, frontend construction,
context watchers, message limits, notice/notification/error callbacks, OAuth token provision,
SSL key-password callbacks, Kerberos configuration, protocol limits, channel binding, and
required authentication methods.
At the pgx layer also preserve tracing, statement and description caches, and query modes.

### Parsing and application supplied options

Share lexical parsing where its behavior agrees, then build distinct legacy and new
configurations. Do not build a legacy configuration and try to reverse its flattened output.

Support application-supplied connection parameters before and after the connection string,
inspired by [PQconnectdbParams](https://www.postgresql.org/docs/18/libpq-connect.html#LIBPQ-PQCONNECTDBPARAMS).
Use ordered key/value entries for defaults and overrides if duplicate parameters are exposed.
Define duplicate and empty-value behavior explicitly. Combine these inputs before deriving
hosts, TLS policy, or authentication settings. Keep function-valued Go hooks in typed options.

Resolve environment and service settings with the pinned libpq precedence and snapshot the
resolved inputs during parsing. Avoid silently rereading process environment during later
connections. Credential file paths may remain references; specify when their contents are
loaded or refreshed, including pooled reconnects. TLS and password material should be selected
or constructed with the current host and current edited configuration.

Preserve the existing connection-string key restriction capability in the new parser. Check
untrusted string keys before service-file or credential-file access, including duplicate and
alias spellings. Document whether trusted application defaults and overrides are subject to
a separate restriction; do not weaken existing restrictions on the legacy path.

For the new path, recognize libpq client parameters separately from server runtime parameters.
Reject recognized but unsupported client options with a clear error; never forward them as
startup GUCs. Prefer rejecting unknown connection-string keys, except documented pgx options,
and provide explicit `RuntimeParams` for Go callers plus PostgreSQL's `options` connection
parameter. Document this migration from the legacy parser's arbitrary-key passthrough.

### TLS and authentication customization

Keep SSL policy separate from Go TLS customization. Build a fresh TLS configuration for each
host, including verification identity, SNI policy, and ALPN. Do not reuse a configuration
whose verification callback or `ServerName` was bound to a different host.

The API prototype must choose between a constrained TLS settings object and a customization
callback receiving the derived TLS configuration and destination. Prefer customization after
policy derivation with explicit ownership. Define whether custom verification intentionally
replaces policy or only extends it; reject conflicting combinations rather than silently
weakening a required policy. A customizer must not reenable a prohibited plaintext fallback.

Retain `TargetSessionAttrs` as an explicit policy instead of installing its implementation
into the application's `ValidateConnect` slot. Proposed hook order is transport setup,
authentication/startup, built-in target check, application validation, then `AfterConnect` on
the selected connection. Define application-validation rejection as moving to the next logical
host; a separate explicit abort result may stop the operation. Cancellation always stops it.
Do not reinterpret every application callback error as a libpq transport failure.

Document the extension boundaries: custom transport, verification, or validation behavior can
intentionally differ from libpq. The default path must follow the compatibility matrix.

### Configuration accessors and other typed boundaries

Prototype these boundaries before committing to established-connection return types:

| Boundary | Required contract |
| --- | --- |
| `pgx.Conn.Config()` and `pgxpool.Pool.Config()` | Preserve exact legacy behavior for legacy objects. Provide an exact new configuration accessor for new objects, including `Copy()` and reconnect round trips. |
| `pgconn.ConnectError.Config` | Keep its existing type and legacy behavior. Add a new error representation or typed accessor for new failures; preserve error unwrapping and SQLSTATE discovery. |
| `TraceConnectStartData.ConnConfig` | Provide a new typed tracing event/interface or explicit common metadata. Do not fabricate an old config to satisfy a tracer. |
| `AfterNetConnect` and preconnection hooks | Pass new configuration or explicit attempt metadata where the old signature exposes `*Config`. |
| `HijackedConn`, `Hijack`, and `Construct` | Preserve selected destination, negotiated TLS, runtime callbacks, and configuration origin through extraction and reconstruction. Do not reconstruct a new connection through legacy defaults. |
| Pool configuration and callbacks | Introduce new types for callbacks that can edit connection configuration. Reuse callbacks about established connections where possible. |
| stdlib configuration storage and options | Store and copy the correct new type without routing back through legacy parsing. |

The preferred outcome is to reuse `PgConn`, `Conn`, and `Pool` while separating their private
runtime settings from public legacy configuration. However, the old accessors cannot
truthfully represent every new configuration. Choose and document one of these outcomes in
the API prototype: checked accessors with explicit absence for the other origin, or a new
wrapper exposing the appropriate configuration accessor while sharing implementation.
If retaining shared types would force existing methods into misleading or unexpectedly nil
results, prefer the wrapper. Do not silently return a lossy legacy projection that appears
safe to reconnect. This decision is open; it is not permission to duplicate query machinery.

Likewise, a connection string accessor must state whether it returns original input or a
serialization of effective settings. It must not claim to serialize callbacks or all edits.

## Internal connection architecture

Keep five responsibilities separate:

1. Parse and merge declared settings, preserving required presence information.
2. Validate the editable configuration and construct operation-local policy and material.
3. Select logical hosts, resolve addresses lazily, and run the connection state machine.
4. Perform shared transport, authentication, and protocol operations with explicit attempt
   inputs rather than reading a mutable public legacy configuration.
5. Retain the selected destination, negotiated transport, and required runtime callbacks on
   the established connection for queries, cancellation, and diagnostics.

The legacy scheduler continues to consume legacy configurations. The new scheduler consumes
only the new logical model. Share lower-level operations incrementally where their behavior
is equivalent; do not force both schedulers through a flat list of `FallbackConfig` values.
Do not build a general-purpose public retry framework or arbitrary transport-policy API.

Operation-local state should include the host index, address index, target-selection pass,
allowed and attempted transports, protocol phase, attempt deadline, selected credentials,
and errors. Keep the logical host distinct from the actual network address throughout.

### State machine behavior

Represent transition decisions explicitly, such as continue on the current socket, reconnect
the same address with another transport, advance address, advance host, restart the target
pass, accept, and stop. A phase-aware transition table is preferable to a global list of
SQLSTATEs that always stop or always retry.

Required behaviors include:

- Resolve a host only when reached. Preserve host grouping even when it resolves to several
  addresses. Cache resolution within the scope required by the baseline; confirm second-pass
  resolution behavior rather than assuming the first pass's addresses are always reused.
- Apply transport alternatives to the current address. Continue plaintext on the same socket
  after an allowed SSLRequest refusal; distinguish refusal from TLS handshake failure.
- Classify network, TLS, authentication, and startup errors at the phase where they occur.
  Reproduce the baseline's address/host skipping and transport retry behavior, including
  `57P03`, `53300`, and `28P01` in the relevant phases.
- Evaluate built-in target attributes using startup information when available, querying only
  when necessary. A target mismatch advances to the next logical host.
- Implement `prefer-standby` as the baseline's complete second pass with relaxed preference,
  rather than retrying one saved candidate. Application validation still applies.
- Support `load_balance_hosts` through host ordering and address ordering within each host.
  Randomize operation-local state without rewriting the reusable configuration.
- Identify timeout scopes by host/address position, not equality of formatted address text.
  Preserve the deadline across transport retries within that scope and renew it only at the
  transitions required by the baseline. The caller's context can impose a shorter deadline
  on the entire operation. Pass that context to hooks and require them to honor cancellation;
  Go cannot forcibly interrupt an arbitrary callback, and detached goroutines must not be used
  to disguise a callback that ignores cancellation.
- Close rejected or failed connections, release temporary resources, and cancel attempt
  timers promptly. Once accepted, do not continue background attempts.

These are implementation requirements derived from the earlier investigation; the permanent
transition tests must resolve the exact phase distinctions against the pinned client.

### Selected destination and reusable runtime state

Authentication receives the selected host's credentials and identity. Cancellation uses the
selected TCP endpoint or Unix socket and its transport policy. Never recover these from the
first host in the original configuration. Preserve encrypted cancellation and protocol-version
dependent cancellation keys already supported by pgx.

Decouple post-connect code from `pgConn.config` and `Conn.config` where those fields currently
serve both public configuration and runtime purposes. Notice handlers, context watchers,
query modes, caches, and cancellation still need settings after connection establishment;
they do not need a fabricated legacy connection plan.

Error reports should identify logical host, attempted address, transport, phase, and wrapped
cause where useful. Preserve `errors.Is` for context errors and `errors.As` for server errors.
Do not include passwords, tokens, decrypted key passwords, or unredacted connection strings
in errors or traces. Error wording need not reproduce libpq byte for byte.

## Work across packages

Start the implementation audit in `pgconn/config.go`, `pgconn/pgconn.go`, the `pgconn/auth_*`
files, `pgconn/krb5.go`, and `pgconn/errors.go`; then follow callers through `conn.go`,
`tracer.go`, `pgxpool/pool.go`, and `stdlib/sql.go`. Check all readers of the private connection
configuration fields before replacing their role with shared runtime settings.

| Package | Implementation work |
| --- | --- |
| `pgconn` | New configuration/parsing family, scheduler, transport integration, attempt errors, authentication inputs, and selected-destination state. |
| `pgx` | New connection configuration and constructors, typed connection tracing, reusable runtime settings, configuration round trips, existing cache/query behavior. |
| `pgxpool` | New parse/construction path and configuration types; copied preconnection configuration; initialization, replacement, health, and shutdown behavior. |
| `stdlib` | New typed connector/OpenDB route and options; correct reconnect/reset behavior; explicit registration mechanism if needed. |
| Documentation and examples | New default examples, replacement guidance, migration recipes, and an explicit behavior matrix. |

Keep `sql.Open("pgx", dsn)` and existing registered-config tokens on their existing path. Prefer
the new typed connector as the first stdlib entry point. If DSN-only selection is required,
give it an explicit new driver name or registration scheme rather than changing the old one.
Pool-backed stdlib connections must remain on the pool's selected path. Audit internal uses of
`Config()` so neither adapter switches engines accidentally.

Pool hooks need independent copies of host slices, runtime maps, and mutable TLS material.
Verify that edits affect one physical connection and do not change existing or future pool
connections unless explicitly applied to the pool template. Preserve pool timeout defaults
as a documented pgx policy; distinguish that outer policy from libpq's attempt timeout.

## Scope of libpq parity

The architecture should allow additional libpq parameters without another public connection
system. Full parity is broader than the scheduler. Track each option as implemented, a
documented pgx extension/difference, or explicitly unsupported.

### Required for the first complete new path

- Logical host/address handling, optional-TLS retries, lazy resolution, error transitions,
  timeout scopes, all target-session modes, and complete preference passes.
- `hostaddr`, `load_balance_hosts`, and password-file matching per destination, including
  default database/user handling and platform permission checks.
- Existing pgx transport and authentication capabilities, including direct TLS, SCRAM and
  channel binding, `require_auth`, token-provider OAuth, protocol limits, and cancellation.
- Correct enforcement of channel binding across authentication methods, direct-TLS ALPN,
  SNI versus certificate verification, SSL option validation/defaulting, and protocol
  negotiation validation. These defects must not be inherited by the new path.
- New pgx, pool, and stdlib connector integration, configuration copying, and complete
  documentation of supported hooks and extensions.

### Additional parity work with explicit status

Inventory these families and implement the tractable additions in separate commits. Their
absence must be documented or rejected explicitly; it must not be concealed by an unqualified
claim of full libpq compatibility.

| Family | Items to evaluate |
| --- | --- |
| Environment and defaults | Missing libpq environment variables, platform default locations, `fallback_application_name`, and `client_encoding=auto`. |
| Services | User/system service-file search, `PGSYSCONFDIR`, precedence, and LDAP lookup. |
| Network options | Keepalive controls, `tcp_user_timeout`, Unix sockets and abstract sockets, and `requirepeer`. |
| TLS material and policy | Certificate/key formats and permissions, root trust behavior, CRLs, `sslcertmode`, minimum/maximum TLS versions, key logging, and deprecated options. |
| Authentication | GSS encryption/delegation and SSPI support, SCRAM client/server key options, and OAuth discovery/device-flow settings. |
| Backend-specific behavior | OpenSSL engines/configuration, certificate-name rules, and other semantics that do not directly map to Go TLS. |

GSS encryption, SSPI, built-in OAuth discovery/device flow, LDAP, and OpenSSL-specific features
are independent workstreams, not prerequisites for the core scheduler. Reserve their place
in the capability matrix rather than treating unsupported settings as server parameters.
Do not remove existing Go authentication extension points while these remain unsupported.

For TLS compare against the [SSL documentation](https://www.postgresql.org/docs/18/libpq-ssl.html)
and pinned [OpenSSL integration](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/interfaces/libpq/fe-secure-openssl.c).
For authentication consult the pinned [authentication dispatcher](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/interfaces/libpq/fe-auth.c).
Use the [password-file](https://www.postgresql.org/docs/18/libpq-pgpass.html),
[environment](https://www.postgresql.org/docs/18/libpq-envars.html),
[service-file](https://www.postgresql.org/docs/18/libpq-pgservice.html), and
[OAuth](https://www.postgresql.org/docs/18/libpq-oauth.html) documentation for the corresponding
capability rows. Any deliberate deviation needs a focused test and an explanation.

## Validation strategy

Commit permanent fixtures and tests. Do not depend on temporary audit scripts, an unpinned
system `psql`, personal credentials, external databases, or a live internet connection.

Build a deterministic local protocol simulator that records connections, SSL/GSS negotiation,
startup packets, requested authentication, validation queries, and close events. Use a
controlled resolver for pgx tests and a controlled equivalent host/address setup for libpq.
Make explicit when only pgx's injected resolver is being tested; do not claim that it proves
the oracle's DNS behavior. Keep a seeded random source for deterministic ordering tests.

Use an optional small libpq client helper for differential runs, with pinned build/version
metadata. Normal Go tests must run without a C toolchain or libpq dependency. Convert verified
observations into deterministic Go regression tests and retain an opt-in differential job
to detect drift. Existing parser oracle tests in `pgconn/parse_url_test.go` and
`pgconn/parse_keyword_value_test.go` are useful precedent, not an oracle for the new scheduler.

Required test groups:

| Group | Cases |
| --- | --- |
| Inputs | URL/keyword forms, repeated and empty values, parameter arrays, environment/service precedence, allowed keys, invalid and unsupported parameters. |
| Hosts | Several hosts and addresses, lazy and failed DNS, explicit addresses, socket paths, repeated equal address strings at different positions, and random ordering. |
| Transports | Every supported SSL mode, refusal on the same socket, handshake failure, startup failure after each transport, direct TLS with/without ALPN, and SNI disabled with verification enabled. |
| Selection | Every target attribute, startup status versus query fallback, skipping addresses after target rejection, and complete preference passes including failures on the second pass. |
| Authentication | Per-host passwords, edited user/database, explicit password precedence, file permissions, channel binding with trust/password/SCRAM, and authentication allow/deny policy. |
| Lifetimes | Per-address deadlines, transport retries sharing a deadline, caller cancellation, blocked hooks, cleanup after failure, and no leaked goroutines or sockets. |
| Configuration | Independent copies, parse-modify-connect, TLS changes, exact new round trips, and correct accessor/tracer/error behavior. |
| Integration | pgx execution settings, pool creation/replenishment and hook isolation, stdlib construction/reset/reconnect, hijack/reconstruction, and cancellation to a fallback destination. |
| Legacy | Existing fallback edits and ordering, nil/empty fallbacks, custom TLS and validators, copied configurations, and no implicit engine switches. |

Exercise real PostgreSQL TLS/authentication and supported server versions in addition to the
simulator. Run race tests for concurrent pool construction/copy use, and verify cross-platform
socket, credential-file, and TLS behavior. Use the repository's existing database lifecycle
and test commands from `DEVELOPMENT.md`; do not hardcode local database ports.

## Implementation sequence

Each phase should end in reviewable commits with its relevant tests. Update this plan with
resolved API decisions and the capability matrix as implementation proceeds.

### Phase 1 API and integration prototype

1. Recheck current source and applicable repository instructions; preserve independent fixes.
2. Choose provisional names consistently across pgconn, pgx, pool, and stdlib. Do not spend the
   implementation session reopening the choice of separate public connection paths.
3. Inventory legacy-config coupling in accessors, errors, tracing, pool callbacks, and stdlib.
4. Resolve established-connection type/accessor behavior with small compilable usage examples
   for both paths, including passing new connections through callbacks and adapters.
5. Finalize configuration ownership, optional/default settings, parameter precedence, TLS
   customization, hook rejection/abort semantics, and credential refresh timing.
6. Record those decisions here and establish the new capability matrix.

Exit criterion: coherent typed usage at every layer, no lossy implicit conversion, and no
unresolved accessor contract concealed by returning existing connection types.

### Phase 2 Permanent compatibility tests

1. Add the local protocol simulator and optional pinned libpq helper.
2. Reproduce the earlier observations and derive the phase-aware transition table.
3. Record legacy behavior in regression tests before extracting shared machinery.
4. Separate differences required by the new engine from independent security/correctness fixes.

Exit criterion: tests can distinguish the required new behavior from the legacy behavior,
and the test inputs and oracle version are reproducible from repository instructions.

### Phase 3 Parsing and configuration

1. Add the distinct new configuration types and parse/copy/validation APIs.
2. Share lexical parsers where appropriate and implement the new normalization pipeline.
3. Add host/address/port modeling, explicit policies, parameter defaults/overrides, credential
   sources, and configuration-sensitive TLS construction.
4. Validate unsupported options and preserve intentional pgx options and runtime parameters.

Exit criterion: configuration mutations have predictable effects and no legacy flattened
attempt list or stale derived credential/TLS values are needed by the new path.

### Phase 4 Shared primitives and the new scheduler

1. Extract the minimum shared transport/protocol helpers while keeping legacy tests passing.
2. Give authentication and cancellation explicit selected-destination inputs.
3. Implement the new host/address/transport state machine, deadlines, preference passes,
   ordering, and error aggregation.
4. Enforce TLS/authentication/protocol requirements and add the new low-level constructors.

Exit criterion: the deterministic and differential core matrices pass, cancellation and
cleanup are correct, and the legacy entry points still use legacy scheduling.

### Phase 5 Integration through the full stack

1. Add pgx constructors and configuration, tracing, and accessor integration.
2. Add pool parsing/options/construction and typed hooks with copy isolation.
3. Add stdlib typed connectors and any explicitly selected registration path.
4. Verify query/cache behavior, reconnects, credential refresh, and cancellation across layers.

Exit criterion: a caller can use the new path throughout an application without accidentally
falling back to old parsing or needing to reconstruct a legacy configuration.

### Phase 6 Migration and deprecation

1. Publish examples using the new family as the primary path.
2. Add a migration guide mapping old constructors, options, accessors, hooks, and stdlib usage.
3. Explain semantic changes separately from symbol renaming: retry outcomes, host ordering,
   TLS validation, parameter handling, password files, and target selection.
4. Give explicit recipes for old `Fallbacks` edits. Host selection, transport policy, and
   deliberate custom retry sequences are different intentions and cannot all be autofixed.
5. Add Go `Deprecated:` documentation pointing to specific replacements only once the new
   family and its adapters cover the relevant use cases. Keep legacy-focused tests/examples.
6. Offer lint guidance or a focused migration check; do not introduce a new tooling dependency
   merely to enforce naming. Mechanically transform only cases whose intent is unambiguous.
7. Publish tested support, deliberate deviations, and unsupported features. Do not promise full
   libpq parity or schedule removal of legacy APIs within the current major version.

Exit criterion: humans and agents can discover the new path, migrate ordinary callers, and
recognize cases requiring a behavioral decision without consulting this conversation.

### Phase 7 Remaining compatibility features

Implement additional capability rows in independent changes, using the same configuration
model and comparison strategy. Prioritize practical connection behavior over platform-specific
features with substantial dependencies. Review baseline updates explicitly. Consider removing
deprecated APIs only as a separate major-version decision.

## Completion criteria for the initial implementation

- All new connection entry points select the new engine; all legacy entry points retain
  their documented legacy behavior, subject to separately documented correctness fixes.
- The new configuration retains logical hosts and policies without `FallbackConfig`, hidden
  legacy-detection rules, or global mode state.
- Configuration copying, mutation, accessors, errors, tracing, hooks, pools, and stdlib have
  explicit tested contracts; exact new configuration round trips preserve the new path.
- Core behavior agrees with pinned libpq in the declared supported matrix, including
  transport retries, target passes, deadlines, selected-host credentials, and cancellation.
- TLS/authentication enforcement meets the required policies independently of retry behavior.
- Supported real-server tests, relevant race/platform checks, formatting, and repository lint
  pass. Optional differential checks state their baseline and any required environment.
- The migration guide and deprecation annotations point to working replacements; unsupported
  capabilities and Go-specific extensions are documented without silently changing old code.

Implementation begins in a later session from this plan branch. This commit contains the
architecture and directions only; it does not introduce APIs or deprecations yet.
