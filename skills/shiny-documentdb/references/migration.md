# Upgrading from v10 to v11

Read only the sections relevant to the current task. These are application-facing API details.

## Contents

- Upgrading from v10 → v11

## Upgrading from v10 → v11

**Only relevant when a codebase is on v10** — do this migration when the user asks to upgrade, or when you see
v10-only signals in the code: `CreateUnitOfWork(`, the public `UnitOfWork` type in a signature,
`IDocumentStoreProvider`, `AddDocumentStoreInstrumentation(` / `o.Instrumentation = true`, or a `PackageReference`
to `Shiny.DocumentDb.Extensions.DependencyInjection` / `Shiny.DocumentDb.Diagnostics`. (Do **not** treat normal
v11 API — `IDocumentSession`, `OpenSession`, `AddScopedDocumentSession` — as a migration signal.)

v11 is a clean break (no `[Obsolete]` shims). Apply these mechanical transforms:

| v10 | v11 |
|---|---|
| `PackageReference` to `…Extensions.DependencyInjection` / `…Diagnostics` | **remove both** — folded into core `Shiny.DocumentDb`, namespaces unchanged |
| `var uow = store.CreateUnitOfWork();` | `await using var session = store.OpenSession();` (buffered `Add`/`Update`/`Upsert`/`Remove` + `SaveChanges` identical) |
| `uow.Clear()` | `session.ClearPending()` |
| `UnitOfWork` in a signature (`Action<UnitOfWork,…>`, `void F(UnitOfWork uow)`) | `IDocumentSession` (the public type is now internal) |
| `IDocumentStoreProvider` | `IDocumentSessionFactory` (`GetStore(name)` unchanged; `OpenSession(name)` added) |
| `new AppContext(store)` (generated `DocumentContext`) | `new AppContext(store.OpenSession())`; the context is now `IAsyncDisposable` — `await using` it; `context.CreateUnitOfWork()` → `context.Add`/`SaveChanges` |
| `services.AddDocumentStoreInstrumentation();` / `o.Instrumentation = true` | remove — telemetry is embedded/always-on; subscribe OTel to `.AddSource("Shiny.DocumentDb")` / `.AddMeter("Shiny.DocumentDb")` |

**Session registration by host:** ASP.NET Core → add `.AddScopedDocumentSession()` and inject scoped
`IDocumentSession`; MAUI/desktop/background/Orleans → inject the singleton `IDocumentSessionFactory` and
`OpenSession()` per unit of work (`await using`); immediate one-offs → keep injecting `IDocumentStore`. Ask the
user which host if it isn't obvious. Interceptors don't break (v11 adds `ctx.Services`/`ctx.Session` + scoped
support). **After migrating, build (expect 0 errors) and run the FULL test suite** (needs Docker for non-SQLite
providers; if Docker is off, tell the user to start it). Full guide + before/after code:
[`/documentdb/migrate-v10-to-v11`](https://shinylib.net/documentdb/migrate-v10-to-v11).
