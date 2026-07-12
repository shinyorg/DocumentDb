# Plan: Session-architecture follow-ups (telemetry + scope-aware features)

**Status:** ✅ SHIPPED (branch `v11`, target `11.0.0`). All implemented & green — full solution builds 0 errors;
non-container core suite **964/0**, generator 9/9. Delivered: A1 scope-aware tenancy, A2 scope-aware temporal
`ResolveActor`, T1/T2 unit-of-work span + `db.session.id` correlation, T3 `db.client.unit_of_work.operations`
metric, L1 `BeginTransaction(IsolationLevel)` consistent-read sessions. L2/L3/L4 are no-code (already achievable /
provider-fragile / not-built) — see the lower-value section. Tests: `SessionFollowupTests` (5). Four artifacts
synced (3 feature release notes, SKILL, readme, diagnostics.mdx + temporal.mdx). Builds on the store-as-connection
migration (`docs/plans/store-as-connection.md`) and the interceptor cleanup.

The session now carries the caller's DI scope and has a lifetime. Two families of feature benefit:
**telemetry** (a session is a natural trace scope) and **scope-aware ambient accessors** (tenancy and the
temporal audit actor have the same "ambient `Func` resolving scoped state from the root" smell the interceptors
had — the session fixes them identically).

## Telemetry

### T1 — Session / unit-of-work span as a trace parent
Today each op emits a span; inside a session a `Get`, a `Query`, and a `SaveChanges` emit **sibling** spans —
the trace doesn't show they were one unit of work. A `DocumentSession` lazily starts a `<system>.unit_of_work`
`Activity` on its first operation and stops it on dispose. Child op spans nest under it via `Activity.Current`
(AsyncLocal). **Zero-cost when unobserved** — `ActivitySource.StartActivity` returns null with no listener, so
an unobserved session allocates nothing. Immediate `store.Insert` (no session) is unaffected.

Mechanism: internal `Diagnostics.IUnitScopeSource { Activity? StartUnitActivity(string op); }` on both store
bases (`DocumentStore`, `DocumentProviderBase`) exposes the resolved `db.system.name` + store name; the session
calls it. Explicit `BeginTransaction` reuses the same span (tagged `db.operation.name = transaction`).

### T2 — Session correlation (falls out of T1)
With a session-level activity, every child op shares its `TraceId` automatically — a unit of work is one
correlated subtree. Optionally stamp `db.session.id` (a `Guid` on the session) as a span tag for correlation
across metrics. No new mechanism; a property of T1.

### T3 — Unit-of-work size metric
`SaveChanges` flushing N coalesced buffered ops records `db.client.unit_of_work.operations` (a histogram) —
write-batching / amplification insight the flat per-op model can't show. Recorded on the session's SaveChanges
(buffered count before flush). Zero-cost when unobserved.

**Not touched:** the `OperationTracker` `AsyncLocal<bool> active` re-entrancy guard. It guards the store
re-entering *itself* internally (Insert → implicit unit → tx-store), which happens on the immediate path where
there is no session; converting it to a session-depth counter buys nothing and adds risk. Correct as-is.

## Scope-aware ambient accessors

### A1 — Scope-aware multi-tenancy
`DocumentStoreOptions.TenantIdAccessor` (`Func<string>`) is wired in DI to
`() => rootProvider.GetRequiredService<ITenantResolver>().GetCurrentTenant()` — a captive-dependency footgun
for a request-scoped resolver (only works today if `ITenantResolver` is a singleton over an ambient
`IHttpContextAccessor`). Fix: resolve from the **flowing session scope** —
`() => (DocumentOperationScope.CurrentServices ?? rootProvider).GetRequiredService<ITenantResolver>()...`. The
session must flow its scope on **reads** too (not just writes) so tenant query-filters resolve from it. Result:
a scoped `ITenantResolver` resolves the request's own instance when writing/reading through a session; the
immediate path falls back to root exactly as today. No public API change.

### A2 — Scope-aware temporal `CaptureActor`
`TemporalOptions.CaptureActor` (`Func<string?>`) captures "who made this change" ambiently — same smell. Add
`TemporalOptions.ResolveActor` (`Func<IServiceProvider, string?>`) resolved from the flowing scope; the temporal
sidecar write prefers `ResolveActor(CurrentServices)` over the unscoped `CaptureActor()`. Now a request-scoped
`ICurrentUser` supplies the actor, per unit of work — the headline of the original DI-scoped-interceptors design,
now trivially enabled. Additive API.

## Lower-value (do, but honest about ceiling)

### L1 — Consistent-read sessions (isolation level)
Add `IDocumentSession.BeginTransaction(IsolationLevel)` → `conn.BeginTransactionAsync(isolationLevel)`. With
`RepeatableRead`/`Snapshot` (relational), all reads in the session see a consistent view — read-modify-write and
multi-read consistency without app-level CAS loops. Relational only (document-native/key-partitioned throw).

### L2 — Session-based seeding — **already achievable (documented, no code)**
`IDocumentSeeder.SeedAsync(IDocumentStore, ct)` already lets a seeder call `store.OpenSession()` for a
transactional, atomic seed. Seed *run-once* semantics rely on **idempotency** (the contract already requires
idempotent seeds) plus the marker, not on seed+marker atomicity — so no seeder-API change is warranted. We
document the `OpenSession()` pattern for transactional seeds; forcing a `SeedAsync(IDocumentSession)` overload
would churn the whole seeder ecosystem for a guarantee idempotency already provides.

### L3 — Backup read-consistency — **documented, no risky change**
`IDocumentBackup.ExportAsync` streams the store (it is not a single explicit read transaction), and restore
already supports `SingleTransaction`. Forcing snapshot isolation on the streaming export is provider-fragile
(SQLite ignores it; SQL Server `Snapshot` needs DB-level config) for marginal gain, so it stays out. A user who
needs a strict snapshot can drive the export inside a `session.BeginTransaction(IsolationLevel.Snapshot)` on a
provider configured for it (enabled by **L1**).

### L4 — Replication (`IDocumentReplicator`) — **N/A (not built)**
Design-only (`docs/plans/store-to-store-replication.md`); not implemented, so nothing to session-ify. When built
it should batch each run in a session and use L1 for the consistent-snapshot read. Noted for that effort.

**Net for the lower-value tier:** L1 shipped (real feature); L2/L3/L4 are correctly *no-code* — already
achievable, provider-fragile, or not-yet-built — and are documented as such rather than churned.

## Order & artifacts
Implement A1+A2 (lowest risk, proven pattern), then T1/T2/T3, then L1–L3. Tests per feature; then sync docs
(diagnostics.mdx, multi-tenancy, temporal), SKILL, readme, and `type=feature`/`breaking` release notes.
