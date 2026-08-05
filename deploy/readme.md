# Deploying ShinyDocDbMyAdmin

Run configuration for hosting the admin UI - the ordinary deployment and the public playground.

There is nothing to build here. `.github/workflows/admin-image.yml` publishes
`ghcr.io/shinyorg/shiny-docdb-myadmin` multi-arch (`linux/amd64` + `linux/arm64`) on every push
that touches `src/**`, under both the ordinary and the `demo` tags.

The same digests are mirrored to `docker.io/aritchie/shiny-docdb-myadmin`. GHCR stays canonical -
it is what `AddDocumentDbAdmin` pulls and what everything below quotes - so the mirror is not a
second build: one push writes both repositories, and each registry's tags are manifest lists over
the same blobs. Pull from whichever you prefer; `:latest`, `:<version>`, `:demo` and
`:demo-<version>` all exist on both. The mirror is skipped when `DOCKERHUB_USERNAME` /
`DOCKERHUB_TOKEN` are not configured, so the GHCR publish never depends on it.

| File | What it is |
|---|---|
| `demo/seed-demo.py` | Generates `seed-demo.sql`. Seeded RNG - re-running gives a byte-identical file. |
| `demo/seed-demo.sql` | The playground's opening sample. Documents **plus sidecars**. |
| `demo/seed-demo-volume.sql` | Bulk documents generated from that sample by the admin tool's own generator. Also byte-identical between runs. |

Both `.sql` files are embedded in the image by `ShinyDocDbMyAdmin.csproj`, which is what lets demo
mode build its own database with nothing mounted.

## Running it

There is no compose file here, because there is nothing to compose: one container, no dependencies,
no sidecars. Everything a stack file would carry is a flag.

```bash
# The ordinary tool - everything works, you supply the connections.
docker run -d --name shiny-docdb-myadmin \
  -p 8085:8080 -v shiny-admin:/data --restart unless-stopped \
  ghcr.io/shinyorg/shiny-docdb-myadmin

# The public playground - read-only sample, built on first start.
docker run -d --name shiny-docdb-myadmin-demo \
  -p 8085:8080 -v shiny-demo:/data --restart unless-stopped \
  ghcr.io/shinyorg/shiny-docdb-myadmin:demo
```

The image sets `ShinyDocDbMyAdmin__DataDirectory=/data` itself, so mounting `/data` is the whole of
the persistence story - saved connections, saved queries, the encryption key, uploads, and (in demo
mode) the sample.

## From Docker Desktop

`docker extension install aritchie/shiny-docdb-myadmin-extension` adds a **DocumentDb Admin** tab
that runs exactly the `docker run` above for you, and hands the new container every database
container already running on the machine - credentials read from each one's own environment, wired
in over the `ConnectionStrings__{name}` / `Shiny__DocumentDb__{name}__Provider` pair below.

The app renders inside the tab, in an iframe over the published port - an ordinary same-machine
request, so Blazor Server's SignalR circuit is an ordinary WebSocket. That needs
`ShinyDocDbMyAdmin__FrameAncestors` (below), which the extension sets on the container it creates.
The container itself is an ordinary one - it appears in the Containers list, and uninstalling the
extension leaves it and its volume alone. See [`src/ShinyDocDbMyAdmin.Extension`](../src/ShinyDocDbMyAdmin.Extension).

## Two images, one build

Both come from the same build and share every layer in the registry - the playground is the ordinary
image plus a single `ENV` line:

| | Ordinary | Playground |
|---|---|---|
| Image | `ghcr.io/shinyorg/shiny-docdb-myadmin:latest` | `ghcr.io/shinyorg/shiny-docdb-myadmin:demo` |
| Pinned | `:<version>` | `:demo-<version>` |
| Connections | you add them, or the host declares them | one, built in, read-only |
| Writes | yes | no |
| Assistant | yes | no |

The tags are interchangeable with the flag: `:demo` is only `ShinyDocDbMyAdmin__DemoMode=true` baked
in, so the ordinary image with that variable set behaves identically, and the demo image with
`ShinyDocDbMyAdmin__DemoMode=false` is the ordinary tool. The sample data is embedded in **both** -
about 1% of a 419 MB image - which is what makes that true in either direction.

## Configuring the ordinary deployment

Same environment contract that `Shiny.DocumentDb.Aspire.Hosting`'s `AddDocumentDbAdmin` uses, so an
AppHost and a `docker run` describe the same deployment:

| Aspire | Environment / flag |
|---|---|
| `AddDocumentDbAdmin(port: 8085)` | `-p 8085:8080` |
| `.WithReference(store)` | `ConnectionStrings__{name}` + `Shiny__DocumentDb__{name}__Provider` |
| `.WithDataVolume()` | `-v shiny-admin:/data` |
| `.WithHostPath(host, container)` | `-v /host/path:/databases:ro` |
| `.WithSecretKey(key)` | `ShinyDocDbMyAdmin__SecretKey` |
| `.WithReadOnly()` | `ShinyDocDbMyAdmin__ReadOnly` |
| `.WithoutAi()` | `ShinyDocDbMyAdmin__DisableAi` |

One setting has no Aspire equivalent, because only the Docker Desktop extension wants it:

| Environment | What it does |
|---|---|
| `ShinyDocDbMyAdmin__FrameAncestors` | Lets the UI be embedded in an iframe. Value is used as the CSP `frame-ancestors` policy verbatim (`*` for any embedder, or a specific origin to scope it). |

Blazor's interactive server render mode blocks framing by default - `frame-ancestors 'self'` plus
`X-Frame-Options: SAMEORIGIN` - which is the right default for a tool that can read and write your
databases. Setting this drops **both**. Leave it unset for anything reachable beyond your own machine;
clickjacking protection is not something a public admin tool should be giving up.

Connections declared this way show up already connected, under a **from host** badge, and cannot be
edited or deleted from the UI - they are declared where the container runs, so that is where they
change. A connection string with no matching `Provider` key is ignored, so an unrelated Redis or blob
variable in the same environment does not turn into a junk connection.

A database *file* on your machine is invisible inside the container unless you mount it, and the
connection string has to use the path as the **container** sees it.

**Set `SecretKey` for anything shared.** With no key configured the app generates one *next to* the
database it protects, which guards against a stray backup or a synced folder but not against anyone
who can read the data directory.

## Demo mode

`ShinyDocDbMyAdmin__DemoMode=true` - already set by the `:demo` tag - is the whole playground
deployment. On first start the app builds its own SQLite sample from the two `.sql` files embedded in
the image, publishes it as a read-only connection, and closes what a public instance should not offer.

The sample is written **once**. If `demo.db` is already on the volume it is left completely alone, so
an image update never discards a database someone is looking at, and nothing in the UI offers a
rebuild - a visitor cannot wipe what everyone else came to see. Deleting `demo.db` from the volume is
the only way to rebuild it, which is deliberately a decision made on the host rather than a button on
a page.

| Closed in demo mode | Still open |
|---|---|
| Editing data (the sample is read-only) | Browsing, searching, filtering |
| Adding, editing or removing connections | The query console, filter grammar and read-only SQL, with plans |
| Importing data | Structure, indexes and statistics |
| Importing or exporting settings | History, geometry, blobs, full text |
| The AI assistant | **Exporting data** - JSON, NDJSON, envelope JSON, CSV |
| Rebuilding the sample | |

A band across the top of the UI says the instance is a demo and expands to list exactly this, so a
visitor knows the missing buttons are policy rather than breakage.

Exporting data stays open on purpose: it is most of what someone is there to try, it only reads, and
the data is a published sample.

> **Note:** demo mode publishes a single read-only connection, so the earlier Demo-plus-writable-
> Sandbox arrangement no longer applies - editing and the Generate tab are closed along with
> everything else that writes. If you want a writable playground back, run the ordinary image and
> declare the connections yourself.

## Two files, two jobs

`seed-demo.py` writes the **varied sample**: a couple of hundred documents chosen for spread rather
than volume, plus the history, blob and spatial sidecar rows that make those tabs appear at all.
Small on purpose - it is what everything else is derived from, and it stays under
`DocumentAdminService.SchemaSampleSize` so the shape is profiled from every document.

`tools/ShinyDocDbMyAdmin.DemoVolume` then loads that SQL into a scratch database and runs the
**tool's own `DocumentGenerator`** over it - the same code the Generate tab runs - to produce several
thousand more documents, emitted as `seed-demo-volume.sql`. So the bulk of the playground is
literally what the tool would make from the sample, and there is no second implementation of
"documents that look like these" to drift out of step with the real one.

```bash
python3 demo/seed-demo.py > demo/seed-demo.sql
dotnet run --project ../tools/ShinyDocDbMyAdmin.DemoVolume
```

Order matters: the volume is profiled from the sample, so regenerate the sample first.

Two things worth knowing about the generated half:

- **No sidecars.** The generator writes through the schema-free collection lane, which maintains
  none - so generated rows have no temporal history and no blobs. History and Blobs are populated
  entirely by the sample. That is the documented behaviour of the Generate tab too, and the tool
  reports it rather than leaving you to notice.
- **Envelope timestamps are recomputed**, not taken from the write. The write path stamps `UtcNow`,
  which would make the file differ on every run *and* give several thousand rows the same `Updated`
  value - so Browse's default sort would show one flat block. They are derived from a stable hash of
  the document id, spread across the span the sample already covers.

## The SQLite gotchas, and where they went

Two SQLite behaviours used to have to be handled by hand at deploy time. `DemoDatabaseBuilder` now
does both, but they are worth knowing if you ever build the file yourself:

1. **The database must be left in WAL journal mode.** `SqliteDatabaseProvider.InitializeConnectionAsync`
   runs `PRAGMA journal_mode=WAL` on *every* connection. Against a database already in WAL that is a
   no-op read; against one in `delete` mode it must rewrite the file header, and if the file is not
   writable the connection dies with `SQLite Error 8: attempt to write a readonly database`. The
   builder sets WAL and checkpoints the `-wal` sidecar away before the file is moved into place.

2. **A leftover `-wal` / `-shm` sidecar breaks a read-only open.** The builder writes to
   `demo.db.building` and moves it, so a failure part-way through cannot leave a half-written
   database that the next start would find and accept as complete.

Corollary: do not open `demo.db` read-write from the host (a bare `sqlite3 demo.db` does), or you
will leave `-wal`/`-shm` files next to it.

## What the seed contains

200 documents - 90 `Order`, 60 `Product`, 50 `Customer` - plus the three sidecars, so the workspace
tabs that only render when their backing data exists all have something behind them:

| Table | Contents |
|---|---|
| `documents` | The envelope. 200 rows across the three types. |
| `documents_history` | ~555 versions. Every document has at least a v1 `Inserted`; most have an `Updated` chain, and three orders end in a `Removed` tombstone with no envelope row. |
| `documents_blobs` | ~86 payloads: real PNGs and plaintext datasheets on products, real one-page PDFs as order invoices. |
| `documents_spatial_map` + `documents_spatial` | R*Tree bounding boxes. SQLite also creates the `_node` / `_parent` / `_rowid` shadow tables; the explorer classifies all five as sidecars. |

**Geometry** is GeoJSON in the document bodies, at four paths: `Customer.location` (Point),
`Customer.serviceArea` (Polygon), `Order.shipTo.location` (Point) and `Order.fulfillment.route`
(LineString). That is deliberate rather than incidental - `DocumentAdminService.Geometry` reads the
bodies, not the sidecar, because "the sidecar holds only bounding boxes for R*Tree / index pruning
... the GeoJSON in `Data` is the actual value". Every geometry passes the library's own `IsValid`.

**The version chains only move fields a real transition would move.** Rebuilding a body from scratch
per version re-rolls every random field, and the diff then reports that the carrier, tracking number,
tax and total all changed at the moment the order shipped - which tells you nothing. Orders vary only
`status` (and gain `fulfillment` once shipped); products vary price, stock and reviews; customers
vary tier, lifetime value and order count. Comparing two adjacent order versions gives `~1`.

The generator asserts these hold before the file is written - the newest version's `Data` equals the
envelope row's, each `ValidTo` equals the next version's `ValidFrom`, exactly one open version per
live document, and no interval of zero or negative length.

## Why seed at all

The Generate tab learns from documents that are already there - `DocumentGenerator` draws numbers
from the observed range, dates from the observed span, and categorical values from the set actually
used. `AnalyzeShape` throws outright on an empty type:

> There are no '{typeName}' documents to learn from. Insert one first - the generator copies the
> shape of what is already there rather than inventing a schema.

So a playground meant to show off sampling has to open on something. The seed is shaped to give the
profiler a spread worth sampling: numeric ranges, a 540-day date span, weighted categorical sets
(`status`, `tier`, `category`), nested objects, arrays of objects, and fields present on only some
documents (`fulfillment`, `accountManager`, `discontinuedOn`).

Counts stay under `DocumentAdminService.SchemaSampleSize` (200) so the shape is profiled from every
seeded document rather than a slice.

## Keeping the image current

`latest` is a moving tag - `admin-image.yml` republishes it on every push to the default branch that
touches `src/**`. Docker will happily keep running an older local copy of it, which is worth knowing
because the Generate tab only landed in `bf676d2` (2026-07-28): a box holding an image from the day
before shows Import/Export with no Generate section and nothing to explain why.

`docker pull` before recreating, or pin to a version tag (`:12.2.1`, `:demo-12.2.1`) if you would
rather update deliberately.

## Reverse proxy

The app is **Blazor Server**. The UI is driven over a SignalR circuit, so the proxy in front of it
has to pass WebSocket upgrades (`Upgrade` / `Connection` headers) or the page renders once and then
sits dead with no error - the standard
`proxy_set_header Upgrade $http_upgrade; proxy_set_header Connection "upgrade";` pair, or whatever
your proxy calls its WebSocket support.

`Program.cs` calls `UseHttpsRedirection()`, but the container listens on HTTP only
(`ASPNETCORE_HTTP_PORTS=8080`, no HTTPS port configured). With no HTTPS port to resolve, the
middleware logs `Failed to determine the https port for redirect` once and passes every request
through - so terminating TLS at the proxy does not produce a redirect loop.

Uploads ride the circuit, and `Program.cs` raises `MaximumReceiveMessageSize` to 1 MB for them.
If you expect people to upload large SQLite files, raise the proxy's body/buffer limits to match.

## What demo mode closes off

This section used to be a list of warnings about running an open instance. Demo mode is the answer
to all of them, which is why it exists:

- **Visitor-created connections.** Without demo mode, `ProfileStore` saves whatever profile a
  visitor types and the container will dial whatever host they name - an outbound-connection
  primitive against anything your box can reach, including the rest of your LAN. Demo mode refuses
  the save in `ProfileStore` itself, not just in the UI.
- **Uploaded database files.** `DatabaseUploadService` writes into the data directory with no quota,
  and it is reached through the connection editor - which demo mode closes.
- **Shared mutable state.** There is no writable store, so nothing one visitor does is visible to
  the next, and there is nothing to reset.
- **Someone else's API key.** The AI assistant is off, so the settings page that asks for one is not
  reachable.

If you run **without** demo mode on a publicly reachable URL, all four are live and none of them are
bugs - they are what an admin tool does. Put it behind auth, or on an isolated Docker network.
