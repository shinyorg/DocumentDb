# ShinyDocDbMyAdmin.Extension

The Docker Desktop extension for [the admin UI](../ShinyDocDbMyAdmin). A dashboard tab that starts the
admin container, shows the running app inside the tab, and hands it every database container already
running on your machine — already connected.

```bash
docker extension install aritchie/shiny-docdb-myadmin-extension
```

Docker Desktop refuses extensions that are not in the Marketplace until you allow them: **Settings →
Extensions → turn off "Allow only extensions distributed through the Docker Marketplace"**. New
Marketplace submissions are paused while Docker reviews Marketplace security, so that toggle is the
only way in for now.

## How the app ends up in the tab

The tab embeds `http://localhost:{port}` in an iframe. That is an ordinary same-machine request from
the webview, so the SignalR circuit Blazor Server needs is an ordinary WebSocket — nothing goes through
the extension's socket proxy, which carries `fetch` calls only. (pgAdmin's extension does the same
thing; it is the established shape for a containerised web app.)

Two things had to be true for that to work:

- **The container is not the extension's `vm` service.** Declaring it there would put it in a hidden
  Compose project whose lifecycle Docker Desktop owns and tears down on uninstall, taking the data
  volume with it — and it would have no published port to frame. It is an ordinary container instead:
  it appears in the Containers list, it is byte-for-byte the `docker run` the readme documents, and
  uninstalling the extension leaves it and your saved connections alone.
- **Blazor had to be told to allow it.** The interactive server render mode emits
  `Content-Security-Policy: frame-ancestors 'self'` *and* `X-Frame-Options: SAMEORIGIN` by default, so
  the frame would render nothing. `ShinyDocDbMyAdmin__FrameAncestors` (see `Program.cs`) opts out; the
  extension sets it to `*` on the container it creates, and nothing else sets it.

A container started **without** that variable — one from before this existed, or one someone ran by
hand — cannot be framed. The tab detects that by reading `Config.Env` rather than trying to observe the
block (a refused embed just renders blank, with nothing the parent is allowed to see) and shows a card
explaining it, with **Open in browser** still available.

## What discovery actually does

The tab lists running containers whose image matches a backend the tool can administer — PostgreSQL
(including PostGIS, pgvector, TimescaleDB), MySQL, MariaDB, SQL Server (including Azure SQL Edge),
Oracle Free/XE and CockroachDB. SQLite, SQLCipher and DuckDB are absent by design: they are files, so
there is no container to find.

For each one it reads the credentials out of the container's own environment (`POSTGRES_PASSWORD`,
`MYSQL_ROOT_PASSWORD`, `MSSQL_SA_PASSWORD`, `ORACLE_PASSWORD`, …) and builds the connection string the
matching `ProviderCatalog` descriptor would. Whatever is ticked on **Start** is passed as the
`ConnectionStrings__{name}` / `Shiny__DocumentDb__{name}__Provider` pair that
[`ProvidedConnections`](../ShinyDocDbMyAdmin.Core/Services/ProvidedConnections.cs) reads — the same
contract `Shiny.DocumentDb.Aspire.Hosting` emits, so a connection seeded here behaves exactly like one
an AppHost referenced in, down to being un-editable in the UI.

Addressing goes over the Docker network rather than a published port, so a database that never published
one still works: the admin container joins each selected container's network after start and refers to it
by name. The default `bridge` network is the exception — no DNS there, so those are addressed by IP.

## Layout

```
metadata.json    the dashboard-tab declaration; no vm or host section
Dockerfile       the Marketplace labels, and the three COPY lines that are the whole build
docdb.svg        extension icon
ui/              index.html + styles.css + app.js — no framework, no build step
screenshots/     served to the Marketplace over raw.githubusercontent
```

`app.js` talks to `window.ddClient`, which is the object `@docker/extension-api-client` returns — that
package is a one-line wrapper around the global. Reading it directly is what keeps this image a few
kilobytes of static files instead of a React toolchain living in a .NET repo.

## Two things the extension host does that a browser does not

Both of these produce a tab that renders perfectly and does nothing, with no error anywhere in the UI.
Neither reproduces in an ordinary browser, which is exactly why they cost so much to find.

**The page already has globals, and `ddClient` is one of them.** The host declares it as a `var`, so it
is both a property of `window` and a binding that a top-level `const ddClient = …` in a classic script
collides with — *"Identifier 'ddClient' has already been declared"*, thrown at instantiation, so the
entire file never runs. Everything here lives inside an IIFE and declares nothing globally; `discovery.js`
publishes exactly one name. `check.js` enforces both:

```bash
node src/ShinyDocDbMyAdmin.Extension/check.js
```

It compiles the host's prelude and both files as a single script, which is how a page does it. (Do not
reach for `vm.runInContext` here — each call gets its own lexical scope, so cross-script collisions
cannot happen and the check passes anything you give it. That was the first attempt.)

**`docker.cli.exec` is not an argv — it is a shell command line.** The SDK joins the command and its
arguments with spaces into one string and refuses the result if `shell-quote` finds an operator in it:
*"shell operators are not allowed when executing commands through SDK APIs"*. So an argument
containing `;`, `&`, `|`, a redirect, a quote, a space, or a glob is either rejected outright or
silently split. Two of those bit us:

- `docker ps --format '{{json .}}'` contains a space, and splits. `scan()` uses `docker ps -q` and
  then `docker inspect` instead — ids cannot be split, and the provider match reads `Config.Image`
  from the inspect output anyway.
- A bare `*` is a **glob**, so `-e …FrameAncestors=*` is rejected. The app accepts `any` as an equal
  spelling, and that is what the extension sends.
- **Every relational connection string contains `;`**, so seeded connections cannot be passed
  literally at all. They go over `Shiny__DocumentDb__{name}__ConnectionStringBase64`, which
  `ProvidedConnections` decodes — base64's alphabet contains no operator, glob or space.

`docker()` in `app.js` screens every argument against that character set before calling the SDK, so a
future call site gets a message naming the offending argument rather than an opaque failure quoting
the whole command line.

## Working on it

```bash
docker build -t shinyorg/shiny-docdb-myadmin-extension:13.0.0 src/ShinyDocDbMyAdmin.Extension
docker extension install shinyorg/shiny-docdb-myadmin-extension:13.0.0 --force
docker extension dev debug shinyorg/shiny-docdb-myadmin-extension   # opens devtools on the tab
```

`docker extension update <image>` reinstalls after a rebuild. `docker extension rm <image>` removes it.

Before a Marketplace submission, run the real validator — it checks things the CI workflow cannot,
and it needs Docker Desktop:

```bash
docker buildx build --platform linux/amd64,linux/arm64 --provenance=false --sbom=false \
  --load -t shinyorg/shiny-docdb-myadmin-extension:13.0.0 src/ShinyDocDbMyAdmin.Extension
docker extension validate shinyorg/shiny-docdb-myadmin-extension:13.0.0
```

The icon and screenshot URLs are `raw.githubusercontent` links into this repo, so they only resolve
once the commit adding them is pushed — expect those checks to fail against an unpushed working tree.
