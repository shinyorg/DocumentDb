# Sample.Aspire — AppHost-gated seeding

A three-project Aspire app that exists to show **when** `WithSeeder` runs.

| Project | Role |
| --- | --- |
| `Sample.Aspire.AppHost` | Provisions Postgres, layers a DocumentDb store on it, and registers the seeder. |
| `Sample.Aspire.Api` | Provider-agnostic consumer — `AddDocumentStore("catalog")` plus two read endpoints. |
| `Sample.Aspire.Domain` | The models, the shared store mapping, and two `IDocumentSeeder`s. |

## Run it

```bash
dotnet run --project samples/Sample.Aspire/Sample.Aspire.AppHost
```

Docker is required (Postgres runs in a container). Open the dashboard link the AppHost prints, then hit
the API's `/products` and `/categories`.

## What to actually look at

The seeder fires on **two triggers only**: the first time this database is set up, and a destructive
recreation. Watch the `catalog-server` logs across runs:

```
run 1   [seed] catalog (Postgres) recreate=False applied=[catalog.categories, catalog.products]
run 2   (nothing — "DocumentDb store 'catalog' was already seeded at … — skipping")
run 3   (nothing)
```

The "already seeded" marker is a `__shiny_documentdb_aspire_seed` row kept **inside the catalog
database**, not in AppHost state. That is what makes the two triggers one mechanism — destroy the data
and the marker goes with it:

```bash
docker volume rm sample-aspire-catalog     # with the AppHost stopped
dotnet run --project samples/Sample.Aspire/Sample.Aspire.AppHost
# [seed] catalog (Postgres) recreate=False applied=[catalog.categories, catalog.products]
```

That is also why the AppHost declares Postgres with `.WithDataVolume(...)` instead of using the
`AddPostgresDocumentStore("catalog")` one-liner. Without a volume the container is discarded on every
stop, so every run would be a first-time setup and the gate would look inert.

The **pinned password** next to it is load-bearing for the same reason. `AddPostgres` generates a fresh
random password every run; the volume keeps the one the database was initialised with. Leave the password
to chance and run 2 fails authentication, the container never goes healthy, and the seeder never receives
its ready event — the run that's supposed to demonstrate the skip instead demonstrates a hang. Any Aspire
database with a persistent volume needs a stable password.

### Rebuild on every start

While you're iterating on seed data, swap the registration in `Program.cs`:

```csharp
.WithSeeder(SeedAsync, DocumentStoreSeedMode.Recreate);
```

Now run 1 still reports `recreate=False` (empty database — nothing to clear) and every run after reports
`recreate=True`, taking the `IDocumentMaintenance.ClearAll` branch first. The AppHost issues no
destructive DDL of its own; it tells the callback which trigger fired and the callback — which holds a
real store — does the wipe.

## Two things the sample is deliberately showing

**The seeder and the service must agree on the mapping.** They build separate `IDocumentStore`s over the
same database, so a mismatched `Table` means the seeder fills tables the API never reads and the API
comes up looking empty. `CatalogStore.ConfigureCatalog()` is shared by both for exactly that reason.

**Version bumps don't reach past the Aspire gate.** `DocumentSeedRunner` keeps its own per-seeder marker,
so the two gates nest: Aspire decides whether this start seeds at all, then the runner decides which
seeders are new enough. Bumping an `IDocumentSeeder.Version` alone will *not* re-seed a database that has
already been through the Aspire gate — use `Recreate`, drop the volume, or seed from the service with
`AddDocumentSeeder` instead. `WithSeeder` earns its place when the data must land *before* dependents
start, or needs credentials the service doesn't have.

## Switching the backend

The API names no provider. Swap the AppHost's two lines and nothing downstream changes:

```csharp
var store = builder.AddSqlServerDocumentStore("catalog").WithSeeder(SeedAsync);   // or MySql / Sqlite
```

The seed callback is the one place that does name a provider (it constructs the store directly), so
switch `PostgreSqlDatabaseProvider` there to match.
