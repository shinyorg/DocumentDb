# ShinyDocDbMyAdmin.Tui

A terminal front end for [Shiny.DocumentDb](https://github.com/shinyorg/DocumentDb) stores. Connect to a
database, browse the documents in it, edit them, query it with DocumentDb's own filter grammar or raw SQL,
read query plans and manage indexes, search full text, inspect geometry, blobs, history and vectors, ask an
AI assistant about the data, and move data in and out. No user management, no server administration - just
the documents.

```bash
dotnet tool install -g ShinyDocDbMyAdmin.Tui
shinydocdb
```

**It is a big install**: ~150MB packed, ~400MB unpacked, almost all of it native provider binaries for
every RID. DuckDB is 300MB of that on its own. A `dotnet tool` is RID-agnostic, so it carries every
platform's binaries or none - the same arithmetic that keeps the web front end container-only. The
difference is that a terminal tool is worth installing anyway.

**Full documentation, screenshots and the keyboard map:
<https://shinylib.net/documentdb/admin/terminal>**

Works against every relational DocumentDb backend - SQLite, SQLCipher, DuckDB, PostgreSQL, SQL Server,
MySQL, MariaDB, Oracle 23ai+, CockroachDB. The document stores (MongoDB, Cosmos DB, LiteDB, IndexedDB, …)
are deliberately out of scope: this tool works against the shared `Id / TypeName / Data / CreatedAt /
UpdatedAt` envelope over ADO.NET, which only the relational providers expose.

## It is the same tool as the web one

[`ShinyDocDbMyAdmin`](../ShinyDocDbMyAdmin) is the browser front end and ships as a container image. Both
sit on `ShinyDocDbMyAdmin.Core`, which holds the connection profiles, the document/schema/index/sidecar
services and import/export - so this is not a reimplementation that agrees on a file format. It is the same
`ProfileStore` over the same `~/.shinydocdbmyadmin/admin.db`, the same `SecretProtector`, the same
`ConnectionTransferService`. A connection saved in one is a connection the other opens.

`tests/ShinyDocDbMyAdmin.Tui.Tests` exports a bundle through this app's services and imports it through the
web app's, and back, because that round trip is the claim worth testing.

**Secrets do not cross machines.** The protector's key is per instance, so a containerised web app and a
tool on the host cannot read each other's stored credentials even pointed at the same volume. The
passphrase-encrypted bundle is the bridge, which is what it was designed for.

## Command line

```
shinydocdb [options]                 open the connection list
shinydocdb --profile <name|id>       open straight into a connection
shinydocdb export <file> [--secrets] write the connection bundle and exit
shinydocdb import <file>             read a connection bundle and exit
shinydocdb help | version
```

`export` and `import` never draw anything - they are the scriptable half of the transfer screen.

## Layout

```
src/ShinyDocDbMyAdmin.Tui/
  Cli/         argument parsing and the two headless verbs
  Shell/       the frame: menu, explorer, screen stack, status line, dialogs
  Screens/     one per page of the web front end
  Panels/      one per workspace tab, plus the two query consoles
  Widgets/     the pieces with no terminal equivalent - the JSON editor, the map, the grids
```

Built on [XenoAtom.Terminal.UI](https://github.com/XenoAtom/XenoAtom.Terminal.UI).

Three things are worth knowing before changing this:

* **Every state assignment happens on the render thread.** Database work runs on the pool through
  `AdminShell.RunAsync` and comes back through `Post`; assigning to a `State<T>` from a worker races the
  renderer. `Pending` is a plain counter assigned into a state rather than incremented in it, because the
  binding layer refuses a read-then-write of the same value inside one tracking context.

* **`AdminShell.Build` composes; `Start` begins work.** Keeping them apart is what lets the render tests
  drive loading deterministically instead of racing a background task.

* **`[Bindable]` types must be in a namespace.** The source generator emits `namespace <global namespace>`
  otherwise, which does not compile and does not say why.

## Running it from a clone

```bash
dotnet run --project src/ShinyDocDbMyAdmin.Tui
```
