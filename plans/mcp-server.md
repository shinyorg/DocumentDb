# Plan: MCP server (`Shiny.DocumentDb.Mcp`)

**Status:** Designed, not started.
**Target version:** `12.6` (new package + new dotnet tool; no core changes).
**Packages:** `Shiny.DocumentDb.Mcp` (library, hostable in any ASP.NET app) and
`ShinyDocDbMcp` (dotnet tool, stdio transport).

> Self-contained build spec. Read `CLAUDE.md` (repo root) for the four-artifact rule (code+tests, docs site,
> skill, readme) before considering any commit "done". Branch off `v12`.

---

## Goal

Point Claude Code / Claude Desktop / Copilot / any MCP client at a Shiny.DocumentDb store and let it explore and
query the data safely:

```jsonc
// .mcp.json
{ "mcpServers": { "documentdb": { "command": "shiny-documentdb-mcp", "args": ["--profile", "prod-readonly"] } } }
```

```
> which orders over $5k are still unfulfilled?
  → documentdb__query_order { filter: "total > 5000 and status != 'fulfilled'", take: 20 }
```

## Why this is mostly assembly, not invention

`Shiny.DocumentDb.Extensions.AI` already generates the hard part: `AITool`/`AIFunction` instances per registered
document type — query, get-by-id, count, aggregate, insert, update, delete — plus the schema-free JSON-collection
lane, a JSON-schema builder for the arguments, a filter translator, path guards, page-size caps, and (shipped in
`11.3`) a **non-removable per-type `Where` scope** the model can neither see nor override. MCP tools are
`AIFunction`s with a different envelope: `McpServerTool.Create(aiFunction)` in the official
`ModelContextProtocol` SDK takes them directly.

So this package is: transport + resources + prompts + a connection story + the safety defaults that make sense
when the caller is an arbitrary agent rather than your own `IChatClient`.

## Non-goals

- **No new query surface.** Anything the MCP server can do, `Extensions.AI` can already do. If a capability is
  missing, it gets added there and both lanes gain it.
- **No raw SQL tool.** Not even read-only, not even opt-in, in v1. The string-expression grammar and the OData
  lane are expressive enough and are already sandboxed by the per-type scope.
- **No schema mutation.** No create/drop table, no index management. The admin tools own that, interactively.
- **No credential storage of its own.** Connections come from the existing admin profile store or from
  configuration.

## Design decisions (locked)

| Decision | Choice | Consequence |
|---|---|---|
| Tool source | Reuse `DocumentStoreAITools` verbatim | One implementation, one security model, one set of tests. A fix to the filter translator fixes both lanes. |
| Scope filters | Static `Where(...)` **plus** request-resolved `Where<TService>(...)` — the addition lands in `Extensions.AI`, not here | Per-caller tenancy on the HTTP transport. Both lanes gain it; MCP only supplies the ambient `IServiceProvider`. Fails **closed**. |
| Default capabilities | `DocumentAICapabilities.ReadOnly` | Writes require explicit opt-in per type in configuration. The safe default is the one you get by doing nothing. |
| Transports | stdio (tool) + Streamable HTTP (library) | stdio for local desktop clients; HTTP for a server the team shares. |
| Connection source (tool) | `ShinyDocDbMyAdmin.Core` profile store | Already ships connection profiles, provider resolution, and `SecretProtector` for the two admin front ends. Zero new secret handling. |
| Resources | Type list, per-type JSON schema, store stats | Lets a model orient itself in one round trip instead of guessing tool arguments. |
| Write confirmation | MCP **elicitation** when the client supports it; otherwise writes stay off unless explicitly enabled | An agent should not silently delete a customer. |
| AOT | Library is AOT-clean; the tool ships self-contained | Matches the repo's `IsAotCompatible` default. Expect a large packed tool (the TUI packs ~152 MB) — measure and note it. |

---

## Surface

### Library

```csharp
// Program.cs of an existing ASP.NET app that already has AddDocumentStore(...)
builder.Services.AddDocumentDbMcpServer(mcp =>
{
    mcp.AddType<Order>(AppJsonContext.Default.Order, capabilities: DocumentAICapabilities.ReadOnly, t => t
        .Where(o => o.TenantId == "acme")                                  // static, invisible — the model cannot lift it
        .Where<ITenantContext>((tenant, _) => o => o.TenantId == tenant.TenantId)   // resolved per tool call
        .IgnoreProperties(o => o.InternalNotes)
        .MaxPageSize(50));
    mcp.AddCollection("intake-forms", capabilities: DocumentAICapabilities.ReadOnly, c => c.AllowAnyField());
    mcp.ExposeResources();                          // documentdb://types, .../schema, .../stats
});

app.MapDocumentDbMcp("/mcp").RequireAuthorization("mcp");
```

`AddDocumentDbMcpServer`'s builder **is** `IDocumentAIToolBuilder` plus a couple of MCP-only knobs — do not fork
the builder interface; extend it.

### Tool

```
shiny-documentdb-mcp --profile prod-readonly            # a saved admin profile
shiny-documentdb-mcp --provider sqlite --connection "Data Source=app.db" --types Order,Customer
shiny-documentdb-mcp --config ./documentdb-mcp.json     # types, scopes, capabilities
```

Late-bound types are the interesting problem: the tool has no compiled `Order` class. Two lanes, both already
supported by `Extensions.AI`:

1. **JSON-collection lane** — `AddCollection(name)` needs no CLR type and no `JsonTypeInfo`. This is the default
   for the tool, and it is why the collection lane exists.
2. **Type discovery from the store** — enumerate stored `TypeName` discriminators (the admin Core already does
   this for its browse UI) and expose each as a collection with fields sampled from stored documents (again,
   the admin's shape-based generator already samples shapes).

### Resources

| URI | Content |
|---|---|
| `documentdb://types` | Discriminators, document counts, mapped indexes/computed/full-text/vector properties |
| `documentdb://types/{name}/schema` | JSON Schema from the existing `SchemaBuilder` (or sampled for a collection) |
| `documentdb://types/{name}/sample` | A few redacted documents — the fastest way for a model to learn the shape |
| `documentdb://stats` | Provider, capability flags (`SupportsVector`/`SupportsFullText`/`SupportsSpatial`), sizes, index stats (reuse the admin's per-provider index-stats tiers) |

### Prompts

- `explain-collection` — "describe what lives in {type} and how to filter it"
- `build-filter` — turns a natural-language ask into a string-grammar filter, with the grammar inlined

---

## Safety model (the part that matters)

1. **Allowlist only.** A type or collection not registered is invisible. Already the `Extensions.AI` contract.
2. **Non-removable scope.** `.Where(...)` per type is applied to query/count/aggregate as a push-down predicate
   and enforced in-memory for get/delete/insert/update. Shipped; reuse, do not reimplement. Extended here with
   request-resolved filters — see below.
3. **Read-only default**, per-type write opt-in, and a global `--allow-writes` that must *also* be set for the
   tool. Two locks on the destructive path.
4. **Page caps** (`MaxPageSize`, default 100) so a model cannot pull a table into its context.
5. **Property hiding** (`IgnoreProperties`) for secrets/blobs; encrypted properties (see
   [field-level-encryption](./field-level-encryption.md)) are excluded automatically.
6. **Audit** — every tool call logged with type, operation, argument digest, affected count, and duration, via
   the store's existing `ActivitySource`/`Meter` plus `ILogger`. An MCP server with no audit trail is not
   deployable.
7. **No ambient credentials in tool arguments.** Connection details come from the profile/config only.

---

## Request-scoped filters resolve services from DI

`11.3`'s scope is a **static** expression fixed at registration (`o.TenantId == "acme"`). That is enough for a
single-tenant server and useless for a shared one: the answer to "which rows may this caller see" lives in an
`ITenantContext` / permission service that only exists per request. So the scope gains a resolved form, in the
same shape the REST endpoints use (see [aspnetcore-endpoints](./aspnetcore-endpoints.md) — keep the two
surfaces named alike; they are the same idea in two envelopes).

**The change belongs in `Extensions.AI`, not in this package.** The locked decision above is "reuse
`DocumentStoreAITools` verbatim"; forking a filter concept into the MCP layer breaks it and leaves the
`IChatClient` lane without a fix it equally wants. `Shiny.DocumentDb.Mcp` contributes exactly one thing: making
sure the per-call `IServiceProvider` reaches the tool.

### Surface (`IDocumentAITypeBuilder<T>`, additive)

```csharp
t.Where(o => o.TenantId == "acme")                                              // shipped 11.3, unchanged
 .Where(ctx => o => o.Region == ctx.GetRequiredService<ICurrentUser>().Region)  // resolved per call
 .Where<ITenantContext>((tenant, ctx) => o => o.TenantId == tenant.TenantId)    // resolve-a-service form
 .Where<IPermissionService>(async (perms, ctx) =>                               // async form
 {
     var ids = await perms.GetVisibleCustomerIdsAsync(ctx.CancellationToken);
     return o => ids.Contains(o.CustomerId);
 });
```

```csharp
public readonly struct DocumentAIFilterContext
{
    public IServiceProvider Services { get; }        // the per-call scope
    public AIFunctionArguments Arguments { get; }    // for correlation/audit — NOT a source of scope values
    public CancellationToken CancellationToken { get; }
    public TService GetRequiredService<TService>() where TService : notnull;
    public TService? GetService<TService>();
}
```

The JSON-collection lane gets the same overloads with the string grammar
(`Func<DocumentAIFilterContext, string>` / `ValueTask<string>`), since collections filter by string.

### Where the `IServiceProvider` comes from

The seam already exists on both sides and needs no new plumbing:

- `AIFunctionArguments.Services` (M.E.AI abstractions) — "services optionally associated with these arguments".
  `DocumentAIFunctionBase.InvokeCoreAsync` already receives the `AIFunctionArguments`.
- `RequestContext<CallToolRequestParams>.Services` (MCP SDK) — the per-request scope; the SDK binds
  `IServiceProvider` parameters from it, and `AIFunctionMcpServerTool` flows it onto the `AIFunctionArguments`
  it builds.

**Build-time verification, first task of the feature:** assert with an integration test that a tool called over
the SDK sees a non-null `arguments.Services` resolving *scoped* services. If a given SDK version does not
forward it, the fallback is a thin `McpServerTool` wrapper in this package that sets
`arguments.Services = request.Services` before delegating — one adapter method, still no forked filter concept.
Do not build the feature on the assumption; verify it first.

For the plain `IChatClient` lane the caller sets `Services` themselves (or uses
`AIFunctionArguments { Services = scope.ServiceProvider }`); document that.

### Rules

- **Fail closed.** A filter that throws, or a `GetRequiredService` that misses, **fails the tool call** with an
  error result. It must never degrade into "run without that predicate" — an unscoped query is the exact
  failure this feature exists to prevent. Same for a null `Services` when a resolved filter is registered:
  hard error naming the type and the missing service, never a silent full scan.
- **Resolve once per call, use for both enforcement paths.** The resolved expression list feeds *both* the
  push-down `ApplyFilters` (query/count/aggregate) and the in-memory `InScope` (get/delete/insert/update)
  within a single invocation. Resolving twice invites the two paths disagreeing — which is a scope bypass.
- **The static fast path stays.** `DocumentAIFunctionBase` interprets `registration.Filters` once in its ctor
  today; split into `staticPredicates` (cached exactly as now) and `dynamicFilters` (resolved + interpreted per
  invocation via the core `ExpressionInterpreter`, compile-free/AOT-safe). A registration with no resolved
  filters pays nothing.
- **Still invisible to the model.** Resolved filters are absent from the JSON schema, absent from the tool
  description, and never echoed in an error message ("no results" — not "you may only see tenant acme"). The
  `11.3` rule extends verbatim; do not let a DI-shaped error string leak the scope.
- **Per-transport reality — say this out loud in the docs:**
  - **HTTP transport (library):** a real per-caller scope. Authenticated principal via `IHttpContextAccessor`
    off `ctx.Services`; this is the multi-tenant story and the reason the feature exists.
  - **stdio transport (the tool):** one process, one OS user, no HTTP identity. A resolved filter there can
    read configuration or a profile, but it is **not** per-caller authorization — do not let the docs imply it
    is. And a `--config` file cannot express a lambda: the tool keeps static filters only; resolved filters
    require hosting the library.
- **Long-running/streaming calls** hold the request scope for the call's duration. Capture values from a
  resolved service, not the service itself.

---

## Tests (`tests/Shiny.DocumentDb.Mcp.Tests`)

- Tool listing matches the registered types/capabilities; an unregistered type produces no tools.
- Read-only registration exposes no insert/update/delete tool.
- The per-type `Where` scope holds on every tool: query (SQL push-down), count, aggregate, get-by-id
  (out-of-scope id → not found, not "forbidden" — do not leak existence), delete/update (refused),
  insert (rejected when the document would fall outside the scope).
- Request-resolved scope (most of these belong in `tests/Shiny.DocumentDb.Extensions.AI.Tests`, with the
  transport ones here):
  - `arguments.Services` is non-null and resolves a **scoped** service when a tool is called through the MCP
    SDK — the build-time verification above, as a standing test.
  - Two calls with different scoped `ITenantContext` values return different rows from the *same* tool
    instance (the tools are built once; the scope is not).
  - A resolved filter ANDs with a static one, and holds on every tool: query push-down, count, aggregate,
    get-by-id (out-of-scope id → not found), update/delete refused, insert rejected out-of-scope.
  - Fail-closed: filter throws → tool error, zero rows returned; unregistered `TService` → tool error;
    `Services` null with a resolved filter registered → tool error. Assert in each case that no unscoped query
    reached the store (spy on the store, not just on the result).
  - The resolved predicate never appears in the tool's JSON schema, description, or any error message.
  - No resolved filters ⇒ the static interpreted-predicate cache is used and `Services` is never touched.
- `MaxPageSize` clamps an over-large `take`.
- Ignored/encrypted properties appear in neither the schema resource nor any result.
- Resource round trip: `documentdb://types` → pick one → `/schema` validates a real document.
- stdio transport: an end-to-end initialize → list tools → call → result exchange against a SQLite fixture.
- HTTP transport: authorization is enforced (401 without the policy).
- Elicitation path: a write tool on a client without elicitation support returns a clear "writes not enabled".

## Four-artifact checklist

- **Code + tests** — as above; add both projects to `DocumentDb.slnx` and `build.slnf`. Note the split: the
  resolved-filter overloads ship in `Shiny.DocumentDb.Extensions.AI` (its own release note, `type="feature"`,
  and an `ai-tools.mdx` section), and this package only guarantees `Services` reaches the tool. They can ship
  in separate versions — `Extensions.AI` first, which de-risks the MCP work.
- **Docs** — new `mcp.mdx` under the existing `ai-tools.mdx` neighborhood: install, `.mcp.json` snippets for
  Claude Code / Claude Desktop / VS Code, configuration reference, the safety model, and a "what the model can
  and cannot see" section. Update `ai-tools.mdx` to point at it (same tools, different envelope). Consider a
  screenshot pass like the admin docs. Release note `type="feature"`.
- **Skill** — a short MCP section (setup + the read-only default + scope rule); `triggers:` += MCP, Model
  Context Protocol, Claude Desktop, agent access.
- **readme.md** — feature bullet + badges for both packages.

## Risks

- **MCP SDK churn.** The C# SDK is young; pin the version and keep the adapter layer thin (`AIFunction` →
  `McpServerTool` is the only coupling that matters). The resolved-filter feature adds a second thing to watch:
  whether the SDK keeps flowing `RequestContext.Services` onto `AIFunctionArguments.Services`. The standing
  test above is the tripwire; the wrapper fallback is the fix.
- **A resolved scope that fails open is a data leak, not a bug.** Every path — throw, missing service, null
  `Services`, a cached predicate list reused across calls — has to end in "no data", and the tests must assert
  at the store, not at the response. This is the single highest-risk item in the plan.
- **Late-bound type discovery quality.** Sampled schemas can mislead a model on sparse documents. Sample more
  than one document, mark inferred fields as inferred, and prefer explicit configuration for anything important.
- **Tool package size.** The TUI tool packs ~152 MB self-contained; a stdio MCP server with 20 provider
  references would be worse. Ship the tool with a **provider subset** (SQLite/PostgreSQL/SQL Server/MySQL/Mongo)
  or as framework-dependent, and measure before publishing.
