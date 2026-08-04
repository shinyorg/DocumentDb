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
        .Where(o => o.TenantId == "acme")          // hard, invisible scope — the model cannot lift it
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
   and enforced in-memory for get/delete/insert/update. Shipped; reuse, do not reimplement.
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

## Tests (`tests/Shiny.DocumentDb.Mcp.Tests`)

- Tool listing matches the registered types/capabilities; an unregistered type produces no tools.
- Read-only registration exposes no insert/update/delete tool.
- The per-type `Where` scope holds on every tool: query (SQL push-down), count, aggregate, get-by-id
  (out-of-scope id → not found, not "forbidden" — do not leak existence), delete/update (refused),
  insert (rejected when the document would fall outside the scope).
- `MaxPageSize` clamps an over-large `take`.
- Ignored/encrypted properties appear in neither the schema resource nor any result.
- Resource round trip: `documentdb://types` → pick one → `/schema` validates a real document.
- stdio transport: an end-to-end initialize → list tools → call → result exchange against a SQLite fixture.
- HTTP transport: authorization is enforced (401 without the policy).
- Elicitation path: a write tool on a client without elicitation support returns a clear "writes not enabled".

## Four-artifact checklist

- **Code + tests** — as above; add both projects to `DocumentDb.slnx` and `build.slnf`.
- **Docs** — new `mcp.mdx` under the existing `ai-tools.mdx` neighborhood: install, `.mcp.json` snippets for
  Claude Code / Claude Desktop / VS Code, configuration reference, the safety model, and a "what the model can
  and cannot see" section. Update `ai-tools.mdx` to point at it (same tools, different envelope). Consider a
  screenshot pass like the admin docs. Release note `type="feature"`.
- **Skill** — a short MCP section (setup + the read-only default + scope rule); `triggers:` += MCP, Model
  Context Protocol, Claude Desktop, agent access.
- **readme.md** — feature bullet + badges for both packages.

## Risks

- **MCP SDK churn.** The C# SDK is young; pin the version and keep the adapter layer thin (`AIFunction` →
  `McpServerTool` is the only coupling that matters).
- **Late-bound type discovery quality.** Sampled schemas can mislead a model on sparse documents. Sample more
  than one document, mark inferred fields as inferred, and prefer explicit configuration for anything important.
- **Tool package size.** The TUI tool packs ~152 MB self-contained; a stdio MCP server with 20 provider
  references would be worse. Ship the tool with a **provider subset** (SQLite/PostgreSQL/SQL Server/MySQL/Mongo)
  or as framework-dependent, and measure before publishing.
