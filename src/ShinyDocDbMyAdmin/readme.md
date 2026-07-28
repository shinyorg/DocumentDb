# ShinyDocDbMyAdmin

A phpMyAdmin-style web front end for [Shiny.DocumentDb](https://github.com/shinyorg/DocumentDb)
stores. Connect to a database, browse the documents in it, edit them, query it with DocumentDb's own
filter grammar or raw SQL, read query plans and manage indexes, search full text, inspect geometry,
blobs, history and vectors, and move data in and out. No user management, no server administration -
just the documents.

Blazor Server, shipped as a container image and nothing else.

```bash
docker run -p 8085:8080 -v shiny-docdb-myadmin:/data ghcr.io/shinyorg/shiny-docdb-myadmin
```

Works against every relational DocumentDb backend - SQLite, SQLCipher, DuckDB, PostgreSQL, SQL Server,
MySQL, MariaDB, Oracle 23ai+, CockroachDB. The document stores (MongoDB, Cosmos DB, LiteDB, IndexedDB,
…) are deliberately out of scope: this tool works against the shared
`Id / TypeName / Data / CreatedAt / UpdatedAt` envelope over ADO.NET, which only the relational
providers expose.

**Full documentation, screenshots, configuration and the Aspire integration:
<https://shinylib.net/documentdb/admin>**

## Running it from a clone

```bash
docker build -f src/ShinyDocDbMyAdmin/Dockerfile -t shiny-docdb-myadmin .
dotnet run --project src/ShinyDocDbMyAdmin
```

There is deliberately no NuGet package of this app. A .NET tool form of it came out at ~120MB -
almost entirely native provider binaries for every RID it might run on - to deliver what one image
delivers for the platform you are actually on.

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

The **one exception** is the filter console, which goes up to `IDocumentStore` instead. JSON
collections (`store.Collection(name)`) are the schema-free lane - no CLR type there either - and the
keying lines up exactly, because a collection's name *is* the `TypeName` column this tool browses
by. So `Collection("Order")` and "the Order type in the documents table" are the same rows, and the
query grammar does not have to be reimplemented here: the console's text goes through the library's
own parser and translator, and shows you the SQL it compiled to. That store is opened with
`SkipTableInitialization = true` - without it every store operation, reads included, runs
`CREATE TABLE IF NOT EXISTS` once per table, which a read-only connection has promised not to do.

## Sidecars

A document write here bypasses the library, so anything the library would have kept in step has to be
kept in step by hand: the temporal `{table}_history` version, the `{table}_blobs` rows, and the
`{table}_vec_{type}` embedding. All three ride in the same transaction as the document write.

**Full text is the exception**: those indexes are maintained by the engine (FTS5 triggers, generated or
computed columns, an on-commit CONTEXT index), so a write from here updates them the same way a write
from the library does - there is nothing to sync. DuckDB is the one backend where that is not true (its
index is a snapshot the library rebuilds before each query), and the Full text tab says so rather than
pretending otherwise.

The vector sidecar has one wrinkle worth knowing. On SQLite it is a `vec0` virtual table, and every
statement against it needs the sqlite-vec extension - which the library loads and this app, which does
not ship the native binaries, does not. So the tool probes the sidecar before opening a transaction
(a failed statement poisons the whole transaction on PostgreSQL, so try-and-catch is not an option)
and, when it cannot write it, reports a stale index rather than pretending it wrote one. The Vectors
tab shows the resulting drift and can rebuild the sidecar from the document bodies wherever the write
path is available.
