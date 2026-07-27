# ShinyDocDbMyAdmin

A phpMyAdmin-style web front end for [Shiny.DocumentDb](https://github.com/shinyorg/DocumentDb)
stores. Connect to a database, browse the documents in it, edit them, run SQL, manage JSON indexes,
and move data in and out. No user management, no server administration - just the documents.

Blazor Server, shipped as a container image and nothing else.

## Running it

```bash
docker run -p 8085:8080 -v shiny-docdb-myadmin:/data ghcr.io/shinyorg/shiny-docdb-myadmin
```

The volume holds saved connections, saved queries and uploaded database files; drop it and the UI
starts empty every time. To reach a database *file* on your machine, mount its directory and use the
container's path in the connection string:

```bash
docker run -p 8085:8080 -v shiny-docdb-myadmin:/data \
  -v /Users/me/databases:/databases:ro \
  ghcr.io/shinyorg/shiny-docdb-myadmin
```

From a clone of this repo, either build the image or just run the project:

```bash
docker build -f src/ShinyDocDbMyAdmin/Dockerfile -t shiny-docdb-myadmin .
dotnet run --project src/ShinyDocDbMyAdmin
```

For an Aspire AppHost, see [In an Aspire AppHost](#in-an-aspire-apphost) below.

There is deliberately no NuGet package of this app. A .NET tool form of it came out at ~120MB -
almost entirely native provider binaries for every RID it might run on - to deliver what one image
delivers for the platform you are actually on.

## Supported providers

Every relational DocumentDb backend:

| Provider | Connection | Notes |
|---|---|---|
| SQLite | file, uploadable | |
| SQLCipher | file, uploadable | needs the encryption key |
| DuckDB | file, uploadable | takes an exclusive lock while open |
| PostgreSQL | server | |
| SQL Server | server | |
| MySQL | server | |
| MariaDB | server | |
| Oracle 23ai+ | server | |
| CockroachDB | server | |

The document stores (MongoDB, Cosmos DB, LiteDB, IndexedDB, …) are deliberately out of scope: this
tool works against the shared `Id / TypeName / Data / CreatedAt / UpdatedAt` envelope over ADO.NET,
which only the relational providers expose.

## What it does

**Explorer** - connection → table → `TypeName`, with live document counts. Tables are classified by
probing for the envelope, so temporal/blob/spatial sidecars are recognised and kept out of the way.

**Browse** - paged, sortable grid with columns inferred by sampling documents. Filters on any
envelope column or JSON path (`=`, `≠`, contains, starts/ends with, comparisons, null checks) and a
quick search across the string fields. Numeric filters and sorts compare numerically, so `9 < 10`
rather than `"9" > "10"`.

**Edit** - JSON editor with format and validation. Insert, edit, duplicate-by-id, single and bulk
delete. Deleting a document also clears its blob sidecar rows.

**Formatted JSON** - any browse row expands into a read-only, syntax-highlighted view of its whole
body, with objects and arrays collapsible (native `<details>` elements, so it works with scripting
off) and a toggle to the raw pretty-printed text. The same view shows a stored version and both sides
of a comparison. Every value is HTML-escaped on the way out: document content belongs to whoever
wrote it, and this markup is rendered raw.

**History** - appears when the table has a `{table}_history` sidecar, written by `MapTemporal<T>`.
Opens on an audit log across the type - what changed, when, by which actor - and narrows to one
document's versions, each with its operation, the interval it was current and how long it stood.
Pick any two versions and compare them: a field-by-field list of what was added, removed or changed
by dotted path (`customer.tier`, `items[2].price`), or the two bodies side by side. A prior version
can be restored behind a two-click confirm, which writes it back as the current document.

**Structure** - the inferred shape of a type (paths, types, how often each field is actually
present, examples), row and size statistics, and one-click create/drop of JSON property indexes,
named exactly as DocumentDb names its own.

**Geometry** - appears when a type stores GeoJSON. Draws the geometries on a server-rendered SVG map
with zoom-to-feature and fit-all, and lists vertices, length, area, centroid and OGC validity per
document. Read from the document body rather than the `{table}_spatial` sidecar - the sidecar holds
only bounding boxes for index pruning and its shape differs per provider, while the GeoJSON in `Data`
is the actual value and is identical everywhere. So this works on every provider, including the ones
with no spatial support at all. Parsing goes through the library's own `GeometryJsonConverter`, so
the tool inherits the real OGC model rather than reimplementing it.

**Blobs** - appears when the table has a `{table}_blobs` sidecar. Lists payloads without ever
selecting the blob column, filterable by document id, with inline preview for images and text and
delete behind a two-click confirm. Downloads stream over a plain HTTP endpoint rather than crossing
the Blazor circuit as base64.

**Import / export** - stream a type out as JSON, NDJSON, envelope JSON (round-trippable) or CSV;
import JSON or NDJSON back with fail / replace / skip handling for duplicate ids.

**SQL** - whatever you type, in the target database's dialect, with `@name` parameters bound from a
JSON box so the types stay honest.

**Create** - a documents table (using the provider's own DDL) and a type (by writing its first
document), so a database whose store has never run isn't a dead end.

## In an Aspire AppHost

`Shiny.DocumentDb.Aspire.Hosting` models this tool as a resource, so it comes up with the rest of
your app and every store you reference is already connected:

```csharp
var store = builder.AddPostgresDocumentStore("orders");

builder.AddDocumentDbAdmin(port: 8085)
       .WithReference(store)
       .WaitFor(store);
```

`WithReference` is the same call a consuming service makes: the tool reads the
`ConnectionStrings:{name}` + `Shiny:DocumentDb:{name}:Provider` pair the hosting integration already
emits. A connection string with no matching provider key is ignored, so a Redis or blob reference in
the same AppHost doesn't turn into a junk connection. Referenced stores show up under a **from host**
badge and can't be edited or deleted from the UI - they're declared in the AppHost, so that's where
they change.

It's the same image either way - running the AppHost and publishing it - tagged to match the hosting
package's own version, so an integration upgrade brings the matching UI with it. Pair it with
`WithDataVolume()` to keep saved connections across runs, and `WithHostPath(hostPath, containerPath)`
to make a file-backed store reachable at all.

## Configuration

| Setting | Environment variable | Default |
|---|---|---|
| `ShinyDocDbMyAdmin:DataDirectory` | `SHINYDOCDBMYADMIN_DATA` | `~/.shinydocdbmyadmin` |
| `ShinyDocDbMyAdmin:SecretKey` | `SHINYDOCDBMYADMIN_KEY` | generated into `<data>/secret.key` |
| `ShinyDocDbMyAdmin:ReadOnly` | `ShinyDocDbMyAdmin__ReadOnly` | `false` |

The data directory holds the tool's own SQLite document store (connection profiles and saved
queries, dogfooding the library) plus any uploaded database files.

`SecretKey` encrypts the secret-bearing parts of a saved profile - connection strings and SQLCipher
keys - with AES-GCM. Either a base64 32-byte key or a passphrase. **Set it out of band for any
shared deployment**: with no key configured, a random one is generated *next to* the database it
protects, which guards against a stray backup or synced folder but not against anyone who can read
the data directory. Connections handed over by a host are never written to that store at all - they
live in memory for as long as the process does.

`ReadOnly` blocks every write path for every host-provided connection, which is the setting you want
whenever the tool is pointed at something you didn't create five minutes ago.

## Safety

Mark a connection **read-only** and every write path is blocked, including non-SELECT statements in
the SQL console. Deleting a connection or bulk-deleting documents takes two clicks. Passwords are
masked wherever a connection string is displayed.

Blob bytes come from whoever wrote the document, so serving them from our own origin is a real XSS
surface. Two rules keep it closed: every response carries `nosniff` and a `default-src 'none'` CSP,
and anything not on the short raster-image allowlist is sent as an attachment rather than rendered.
SVG is excluded from inline display on purpose - it's a document format that can carry script, not
merely an image.

Writes to a temporal-mapped type record a version, using the provider's own history SQL and stamped
with the actor `shiny-docdb-myadmin`. This is not optional bookkeeping: `DocumentAdminService` writes
SQL directly - it sits below `IDocumentStore` because it has no CLR type to bind to - so none of the
library's temporal tracking runs, and without it an edit made here would change the row while the
history sidecar went on insisting the old body was current. A quietly wrong audit trail is worse than
no History tab at all. `Clear` is the one exception, matching the library: a bulk delete is not tracked.

The SQL console is exactly what it looks like: an open prompt against the database. There is no
statement allow-list, so give the tool a database account with the privileges you actually want it
to have.

## Layout

```
src/ShinyDocDbMyAdmin/
  Providers/     ProviderKind, ProviderCatalog - the only place that knows about specific backends
  Models/        Connection profiles, envelope/browse/schema types
  Services/      DocumentAdminService (the provider-agnostic admin layer), profiles, import/export
  Components/    Blazor pages, panels and the explorer tree
```

`DocumentAdminService` sits below `IDocumentStore` on purpose: that API is generic over a CLR
document type, and a tool pointed at someone else's database has no types to bind to. What it does
reuse is `IDatabaseProvider` - `JsonExtract`, `QuoteTable`, `BuildPaginationClause`, and the index
and insert/update builders - so the SQL it emits matches what the library itself would emit, on
every backend, from one implementation.
