# AI tools, access scopes, and MCP servers

Read only the sections relevant to the current task. These are application-facing API details.

## Contents

- AI Tool Integration (Shiny.DocumentDb.Extensions.AI)
- MCP Server (Shiny.DocumentDb.Mcp / ShinyDocDbMcp, 13.0+)

## AI Tool Integration (Shiny.DocumentDb.Extensions.AI)

Expose `IDocumentStore` operations as `Microsoft.Extensions.AI` tool functions for LLM agents.

### NuGet Package

```bash
dotnet add package Shiny.DocumentDb.Extensions.AI
```

### Registration

```csharp
using Shiny.DocumentDb.Extensions.AI;

services.AddDocumentStoreAITools(tools =>
{
    tools.AddType(
        jsonContext.Customer,
        capabilities: DocumentAICapabilities.All,
        configure: b => b
            .Description("Customer records with contact info")
            .Property(c => c.Status, "Active, Inactive, or Suspended")
            .IgnoreProperties(c => c.PasswordHash)
            .Where(c => c.TenantId == "acme")   // non-removable scope — see below
            .MaxPageSize(50)
    );

    tools.AddType(
        jsonContext.Order,
        capabilities: DocumentAICapabilities.ReadOnly
    );
});
```

**No DI?** Build it straight off a hand-constructed store — same builder, returns `DocumentStoreAITools`:

```csharp
using var store = new DocumentStore(options);
var aiTools = store.CreateAITools(tools =>
    tools.AddType(jsonContext.Customer, capabilities: DocumentAICapabilities.ReadOnly));
var chatOptions = new ChatOptions { Tools = aiTools.Tools.ToList() };
```

### DocumentAICapabilities Flags

| Flag | Tool Name Pattern | Description |
|------|------------------|-------------|
| `Get` | `{slug}_get_by_id` | Fetch a single document by ID |
| `Query` | `{slug}_query` | Query with structured filter, sort, paging |
| `Count` | `{slug}_count` | Count with optional filter |
| `Aggregate` | `{slug}_aggregate` | sum/min/max/avg/count |
| `Insert` | `{slug}_insert` | Create a new document |
| `Update` | `{slug}_update` | Replace an existing document |
| `Delete` | `{slug}_delete` | Delete by ID |
| `ReadOnly` | — | Get + Query + Count + Aggregate |
| `All` | — | All seven operations |

### Per-Type Builder (IDocumentAITypeBuilder<T>)

| Method | Description |
|--------|-------------|
| `Description(string)` | Type-level description in tool descriptions and schema |
| `Property<TProp>(expr, string)` | Override description for a specific property |
| `AllowProperties(params exprs)` | Only expose listed properties (allowlist) |
| `IgnoreProperties(params exprs)` | Hide listed properties (blocklist) |
| `Where(Expression<Func<T,bool>>)` | Non-removable scope predicate applied to **every** tool; AND-combined with the model's filter. Call multiple times to combine conditions. |
| `MaxPageSize(int)` | Cap maximum page size for query/aggregate (default 100) |

### Non-removable access filters (`Where`)

`AllowProperties`/`IgnoreProperties` gate which *fields* the LLM sees; `Where` gates which *rows* it can reach. The predicate is a hard server-side boundary the model can't see, disable, or widen past — it's AND-combined with the model's own `filter`. Enforced on every capability:

- `query` / `count` / `aggregate` — pushed into the store query (out-of-scope docs never returned or counted, even if the model names them).
- `get_by_id` / `delete` — an out-of-scope id is treated as "not found" (`found:false` / `deleted:false`); the document is untouched.
- `insert` — a document that would fall outside the filter is rejected (throws) and never written.
- `update` — the incoming document **and** the stored record it replaces must both be in scope (can't move a record out of scope, can't overwrite one the model can't see); returns `updated:false` when the stored record is out of scope.

```csharp
tools.AddType(jsonContext.Order, capabilities: DocumentAICapabilities.All, configure: b => b
    .Where(o => o.TenantId == "acme")
    .Where(o => !o.IsArchived));
```

Evaluated with compile-free, AOT-safe machinery — keep it to LINQ constructs the store can translate. Designed for **stable** scopes (a constant fixed for the singleton registration); for per-request isolation use store-level multi-tenancy / global query filters instead. Scoped `update` re-fetches by `Id`, so it throws on types whose Id is mapped to a differently-named property via `cfg.MapIdProperty` (other tools are unaffected).

### Using the Tools

```csharp
var aiTools = serviceProvider.GetRequiredService<DocumentStoreAITools>();
var options = new ChatOptions { Tools = aiTools.Tools.ToList() };
var response = await chatClient.GetResponseAsync(messages, options);
```

### Structured Filter Format

The query, count, and aggregate tools accept a `filter` JSON object:

```json
// Leaf comparison
{ "field": "age", "op": "gt", "value": 30 }

// Boolean combinators
{ "and": [{ "field": "age", "op": "gte", "value": 18 }, { "field": "status", "op": "eq", "value": "Active" }] }
{ "or": [{ "field": "city", "op": "eq", "value": "Portland" }, { "field": "city", "op": "eq", "value": "Seattle" }] }
{ "not": { "field": "status", "op": "eq", "value": "Cancelled" } }
```

Supported operators: `eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `contains`, `startsWith`, `in`.

### Query Tool Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `filter` | object | — | Structured filter (optional) |
| `orderBy` | string | — | Field name to sort by (optional) |
| `orderDirection` | string | `"asc"` | `"asc"` or `"desc"` |
| `limit` | integer | 50 | Max results (capped at MaxPageSize) |
| `offset` | integer | 0 | Results to skip |

### Aggregate Tool Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `function` | string | `"count"`, `"sum"`, `"min"`, `"max"`, or `"avg"` |
| `field` | string | Numeric field (required for sum/min/max/avg) |
| `filter` | object | Structured filter (optional) |

### Request-resolved scope filters (13.0+)

`Where(predicate)` is fixed at registration. For a scope that differs per caller, use the callback overloads — resolved on **every tool call** from `AIFunctionArguments.Services`:

```csharp
tools.AddType(jsonContext.Order, configure: b => b
    .Where(o => o.TenantId == "acme")                                             // static
    .Where(ctx => o => o.Region == ctx.GetRequiredService<ICurrentUser>().Region)  // resolved per call
    .Where<ITenantContext>((tenant, _) => o => o.TenantId == tenant.TenantId)      // resolve-a-service
    .Where<IPermissionService>(async (perms, ctx) =>                               // async
    {
        var ids = await perms.GetVisibleCustomerIdsAsync(ctx.CancellationToken);
        return o => ids.Contains(o.CustomerId);
    }));
```

Rules when generating this:

- **It fails closed.** A throwing filter, an unresolvable service, a null `Services`, or a null returned predicate all fail the tool call. Never write a `try/catch` that lets a call proceed without the predicate.
- **Deny-all is `o => false`**, never `null`.
- **Set `Services` on the `IChatClient` lane** — `new AIFunctionArguments(args) { Services = scope.ServiceProvider }`. The MCP server does it for you.
- `AddDocumentStoreAITools` throws at **startup** when a `Where<TService>` service is not registered.
- Collections take the same overloads returning a string-grammar clause.

## MCP Server (Shiny.DocumentDb.Mcp / ShinyDocDbMcp, 13.0+)

The same AI tools, exposed over the Model Context Protocol. Two shapes:

```bash
# stdio tool, for a local desktop client
dotnet tool install -g ShinyDocDbMcp
shiny-documentdb-mcp --provider sqlite --connection "Data Source=app.db"
shiny-documentdb-mcp --profile prod-readonly        # a saved ShinyDocDbMyAdmin connection
shiny-documentdb-mcp --config ./documentdb-mcp.json # collections, scopes, capabilities
```

```csharp
// library, Streamable HTTP, for a shared server
builder.Services
    .AddDocumentDbMcpServer(mcp =>
    {
        mcp.AddType(AppJsonContext.Default.Order, capabilities: DocumentAICapabilities.ReadOnly, t => t
            .Where<ITenantContext>((tenant, _) => o => o.TenantId == tenant.TenantId)
            .IgnoreProperties(o => o.InternalNotes)
            .MaxPageSize(50));
        mcp.ExposeResources();      // documentdb://types, .../schema, .../sample, documentdb://stats
        mcp.ExposePrompts();        // explain-collection, build-filter
    })
    .WithHttpTransport();

app.MapDocumentDbMcp("/mcp").RequireAuthorization("mcp");
```

Rules when generating an MCP server:

- `AddDocumentDbMcpServer` **is** the SDK's `AddMcpServer()` — never call both. It returns `IMcpServerBuilder`, so the caller chains `.WithHttpTransport()` or `.WithStdioServerTransport()`.
- Its builder **is** `IDocumentAIToolBuilder` — same `AddType`/`AddCollection`, same per-type configuration.
- **Read-only is the default.** A write capability needs `mcp.AllowWrites()` *as well as* the per-type flag, or registration throws at startup.
- Always set a scope (`Where`) on anything multi-tenant, and prefer the resolved form on the HTTP transport.
- The stdio tool cannot express a lambda scope — static clauses only, declared in `--config`.
