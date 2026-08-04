# Plan: REST + live-query endpoints (`Shiny.DocumentDb.AspNetCore`)

**Status:** Designed, not started.
**Target version:** `12.7` (new package; no core changes).
**Package:** `Shiny.DocumentDb.AspNetCore` — framework reference only (`Microsoft.AspNetCore.App`), no
third-party dependency, AOT-clean.

> Self-contained build spec. Read `CLAUDE.md` (repo root) for the four-artifact rule (code+tests, docs site,
> skill, readme) before considering any commit "done". Branch off `v12`.

---

## Goal

Turn a document type into a complete HTTP resource in one line, including a live tail:

```csharp
app.MapDocuments<Order>("/orders", o =>
{
    o.Operations = DocumentEndpoints.Read | DocumentEndpoints.Write | DocumentEndpoints.Stream;
    o.MaxPageSize = 100;
    o.AllowFilterOn(x => x.Status, x => x.CustomerId, x => x.Total);
    o.TypeInfo = AppJsonContext.Default.Order;      // AOT
})
.RequireAuthorization("orders");
```

```
GET    /orders?filter=status eq 'open' and total gt 500&orderby=total desc&take=20&fields=id,total
GET    /orders/{id}
GET    /orders/count?filter=…
GET    /orders/stream                       # text/event-stream, live
POST   /orders
PUT    /orders/{id}                         # full replace, If-Match honored
PATCH  /orders/{id}                         # JSON Merge Patch (RFC 7396)
DELETE /orders/{id}                         # If-Match honored
```

The store already has every primitive this needs — the string-expression grammar (`Where("…")`,
`OrderBy("…")`), `Project("id,total")` for sparse fieldsets, cursor pagination, JSON Merge Patch
(`Upsert(patch)`), optimistic concurrency with `ConcurrencyException`, and `NotifyOnChange` for the stream.
This package is the HTTP shell over them.

## Read path: build it on the raw JSON lane (v13)

Every GET here reads a document only to write it back out, which is exactly what the **raw JSON terminals**
(v13) exist for. Write the handlers on them from the start — retrofitting later means rewriting every read
handler and its tests.

```csharp
// list — one array straight to the socket, never buffered, never re-serialized
group.MapGet("/", async (HttpContext http, IDocumentStore store, string? filter, int? take, CancellationToken ct) =>
{
    var query = ApplyFilter(store.Query(opts.TypeInfo), filter, opts).Paginate(0, Clamp(take, opts));
    http.Response.ContentType = "application/json";
    await query.WriteJsonArrayTo(http.Response.Body, ct);
});

// by id — the stored body, unparsed
group.MapGet("/{id}", async (IDocumentStore store, string id, CancellationToken ct) =>
{
    var raw = await store.Query(opts.TypeInfo).Where(Scoped(x => x.Id == id)).FirstOrDefaultRawJson(ct);
    return raw is null ? Results.NotFound() : Results.Content(raw, "application/json");
});
```

Rules this imposes on the design:

- **Probe, don't catch.** `IDocumentQuery<T>.SupportsRawJson` is `false` for a type with encrypted properties
  and after a projection. Each read handler picks the lane; the typed path stays as the fallback so an
  encrypted type does not turn a working endpoint into a `501`.
- **`?fields=` keeps using `Project(...)`** — it already returns `JsonObject` natively, so there is nothing to
  save and nothing to change.
- **ETag comes from the version property, which is *in* the body** — read it off the parsed node on the
  by-id path (`FirstOrDefaultJson` rather than `…RawJson`) when `RequireIfMatch` or ETag emission is on.
  Don't materialize a `T` just to read one integer.
- **SSE frames** carry `change.Document` from `NotifyOnChange`, which is already a `T` handed over by the
  change broadcaster — so the stream keeps the typed serialize. Only the request/response reads change.
- **`MapDocumentCollection`** (the schema-free lane) is unaffected: `IJsonDocumentCollection` is already
  JSON end to end.

Both the OData engine (`ODataDocumentQuery.Execute`) and the AI `QueryFunction` were moved onto this lane in
v13 — copy their `SupportsRawJson ? raw : typed` shape rather than inventing a third one.

## Relationship to the OData packages

`Shiny.DocumentDb.OData` + `.AspNetCore.OData` already exist and stay. They are the right choice when the client
speaks OData (`$filter`/`$select`/`$orderby`, `$metadata`, an existing OData toolchain). They also drag in the
OData stack, which is why that package opts out of `IsAotCompatible`.

This package is the other half of the fork: plain REST + JSON, AOT-clean, no dependencies, plus streaming —
for SPAs, MAUI clients, and internal service APIs. **Both must never be mapped on the same route prefix**; the
docs get a short "which one" table.

## Non-goals

- **No SignalR.** Server-Sent Events over `IAsyncEnumerable` needs no package, no hub, no client library, works
  through `EventSource` in a browser and a plain `HttpClient` in MAUI, and reconnects on its own. SignalR can be
  layered by the app if it wants groups/RPC.
- **No durable subscriptions.** The stream is a live tail. Missed events during a disconnect are not replayed —
  a client that needs durability polls `?filter=updatedAt gt …` or uses the change feed directly. Say this in
  the endpoint docs, not just the plan.
- **No scaffolded UI, no OpenAPI document generation of its own.** We emit endpoint metadata; the app's existing
  `AddOpenApi()` renders it.
- **No authorization model.** Endpoints are `RouteGroupBuilder`s — compose `RequireAuthorization`, rate limiting,
  CORS, and output caching the normal way.

## Design decisions (locked)

| Decision | Choice | Consequence |
|---|---|---|
| Return shape | `TypedResults` throughout | OpenAPI metadata for free, AOT-friendly, no `object` boxing. |
| Filtering | The store's **string-expression grammar**, with a per-endpoint field allowlist | One grammar across LINQ, string, OData and AI lanes. Allowlist is mandatory in spirit: an unlisted field in a filter is `400`, not a table scan. |
| Paging | `skip`/`take` **and** `cursor` | `take` is clamped to `MaxPageSize`. Cursor pagination is the documented default for large sets. |
| Concurrency | `ETag` from the mapped version property; `If-Match` on PUT/PATCH/DELETE | `ConcurrencyException` → `412`. Missing `If-Match` is allowed by default, `RequireIfMatch = true` makes it `428`. |
| PATCH semantics | RFC 7396 JSON Merge Patch → `store.Upsert(patch, patchIfUpdate: true)` | The store's merge-patch behavior is the HTTP behavior. No JSON Patch (RFC 6902) in v1. |
| Errors | `ProblemDetails` for everything | `404` not found, `400` validation/parse, `409` duplicate id, `412`/`428` concurrency, `501` unsupported-on-this-provider. |
| Streaming | SSE, heartbeat comment every 30s, `filter=` applies | Proxies kill idle connections; the heartbeat is not optional. |
| AOT | `JsonTypeInfo<T>` threaded through every handler | The store already takes it on every call; do not fall back to reflection. |

---

## Surface

```csharp
[Flags]
public enum DocumentEndpoints { None = 0, Read = 1, Write = 2, Delete = 4, Stream = 8, Count = 16, All = 31 }

public sealed class DocumentEndpointOptions<T> where T : class
{
    public DocumentEndpoints Operations { get; set; } = DocumentEndpoints.Read | DocumentEndpoints.Count;
    public int MaxPageSize { get; set; } = 100;
    public int DefaultPageSize { get; set; } = 25;
    public JsonTypeInfo<T>? TypeInfo { get; set; }
    public string? StoreName { get; set; }                 // keyed store
    public bool RequireIfMatch { get; set; }
    public Expression<Func<T, object>>? DefaultOrderBy { get; set; }
    public TimeSpan StreamHeartbeat { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Fields a client may filter, sort, or project on. Empty = everything (fine for internal APIs,
    /// wrong for public ones).</summary>
    public DocumentEndpointOptions<T> AllowFilterOn(params Expression<Func<T, object>>[] fields);

    /// <summary>Server-side scope AND-ed into every read and enforced on every write — the HTTP twin of the
    /// AI tools' non-removable Where. Typically the caller's tenant or owner id.</summary>
    public DocumentEndpointOptions<T> Scope(Func<HttpContext, Expression<Func<T, bool>>> scope);
}

public static class DocumentEndpointExtensions
{
    public static RouteGroupBuilder MapDocuments<T>(this IEndpointRouteBuilder endpoints, string prefix,
        Action<DocumentEndpointOptions<T>>? configure = null) where T : class;

    /// <summary>Schema-free lane: maps a JSON collection (relational providers only).</summary>
    public static RouteGroupBuilder MapDocumentCollection(this IEndpointRouteBuilder endpoints, string prefix,
        string collectionName, Action<DocumentCollectionEndpointOptions>? configure = null);
}
```

`Scope(...)` is the security-critical member: it runs per request with the `HttpContext`, so it can read the
authenticated principal. Reads AND-it into the query; writes verify the incoming/target document satisfies it
(out-of-scope target ⇒ `404`, never `403` — do not leak existence). This mirrors the AI tools' scope rule, which
is already the repo's established pattern for "the caller cannot remove this predicate".

### Streaming handler sketch

```csharp
group.MapGet("/stream", async (HttpContext http, IDocumentStore store, string? filter, CancellationToken ct) =>
{
    http.Response.Headers.ContentType = "text/event-stream";
    http.Response.Headers.CacheControl = "no-cache";
    var query = ApplyFilter(store.Query<T>(opts.TypeInfo), filter, opts);
    await foreach (var change in query.NotifyOnChange(ct))
        await WriteEvent(http.Response, change, opts.TypeInfo, ct);       // event: insert|update|delete
});
```

Change monitoring is not universal — providers without it throw `NotSupportedException`. Map that to `501` with
a ProblemDetails body naming the provider, and refuse to register `/stream` at startup when
`store is not IObservableDocumentStore` so the failure is at boot, not at 3am.

---

## Tests (`tests/Shiny.DocumentDb.AspNetCore.Tests`, `WebApplicationFactory` over SQLite)

- CRUD round trip incl. `POST` → `201` + `Location`, `PUT` full replace, `PATCH` merge semantics (unspecified
  fields preserved, explicit `null` removes), `DELETE` → `204`, second `DELETE` → `404`.
- Filtering: allowlisted field works; non-allowlisted field → `400` with a message naming the field; malformed
  filter → `400`, never `500`.
- Paging: `take` clamped to `MaxPageSize`; cursor round trip across three pages with a concurrent insert.
- Sparse fieldset `?fields=` returns only those keys.
- Concurrency: stale `If-Match` → `412`; `RequireIfMatch` without header → `428`; ETag changes after update.
- Scope: a document outside the scope is `404` on GET/PUT/DELETE and cannot be created.
- SSE: a client sees insert/update/delete events matching its filter, receives heartbeats, and the enumeration
  stops when the client disconnects (assert the store subscription is disposed).
- `/stream` is not registered on a provider without change monitoring (startup assertion).
- OpenAPI document contains all mapped operations with the right response types.
- Native-AOT publish smoke test of the sample app (the repo already has an AOT test lane — reuse it).

## Four-artifact checklist

- **Code + tests** — as above; new sample under `samples/` (the repo already has `samples/Sample.ODataApi`;
  add `samples/Sample.RestApi` sharing its seeder so the two are directly comparable).
- **Docs** — new `rest-endpoints.mdx`: route table, query-string reference, ETag/concurrency, SSE contract and
  its non-durability, the scope rule, and a "REST vs OData" decision table added to `odata.mdx` too. Release
  note `type="feature"`.
- **Skill** — endpoint mapping section + the "always set `Scope` on a public endpoint" rule; `triggers:` +=
  REST/minimal API/SSE/live query.
- **readme.md** — feature bullet + package badge.

## Risks

- **Filter injection into the string grammar.** The grammar is parsed, not concatenated into SQL, and the OData
  sample already exercised it — but the allowlist and a parse-depth/complexity cap are what keep a hostile
  `filter=` from becoming a resource-exhaustion vector. Cover with tests, including deeply nested boolean input.
- **SSE connection accounting.** One long-lived request per client per resource; without a cap a browser tab
  storm exhausts the thread/connection budget. Document that `/stream` belongs behind rate limiting, and make
  the sample show it.
- **Overlap confusion with OData.** Two ways to do the same thing invites "which is right". The decision table
  must land in the same change, not later.
