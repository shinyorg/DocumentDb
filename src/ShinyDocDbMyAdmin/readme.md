# ShinyDocDbMyAdmin

A phpMyAdmin-style web front end for [Shiny.DocumentDb](https://github.com/shinyorg/DocumentDb)
stores. Connect to a database, browse the documents in it, edit them, run SQL, manage JSON indexes,
and move data in and out. No user management, no server administration - just the documents.

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
