using Sample.Aspire.Domain;
using Shiny.DocumentDb;
using Shiny.DocumentDb.Aspire.Hosting;
using Shiny.DocumentDb.PostgreSql;

var builder = DistributedApplication.CreateBuilder(args);

// A data volume is what makes this sample worth running: without one the Postgres container is thrown
// away on every stop, so every start would be a first-time setup and the seed gate would look like it
// wasn't doing anything. With the volume, the database (and the seed marker inside it) survives.
//
// The pinned password is NOT decoration. AddPostgres generates a fresh random password every run, but
// the volume keeps the one the database was initialised with - so the second run authenticates with the
// wrong password, the container never goes healthy, and the seeder never gets its ready event. A stable
// password is a prerequisite for any Aspire database with a persistent volume. (A literal is fine here
// because this is a local dev sample; use a real secret parameter for anything else.)
var password = builder.AddParameter("catalog-password", "sample-dev-password", secret: true);

var postgres = builder
    .AddPostgres("catalog-server", password: password)
    .WithDataVolume("sample-aspire-catalog")
    .AddDatabase("catalog-db");

// AsDocumentStore rather than AddPostgresDocumentStore, because we modelled the server ourselves above.
// The store name ("catalog") is the handle the API resolves by, and must differ from the DB resource name.
var store = postgres
    .AsDocumentStore("catalog", DocumentProviderKind.Postgres)
    .WithSeeder(SeedAsync);
    // Swap for this to wipe + re-seed on EVERY start while you're iterating on seed data:
    //   .WithSeeder(SeedAsync, DocumentStoreSeedMode.Recreate);

builder
    .AddProject<Projects.Sample_Aspire_Api>("api")
    .WithReference(store)     // ConnectionStrings__catalog + Shiny__DocumentDb__catalog__Provider
    .WaitFor(store);

builder.Build().Run();


// Runs on exactly two triggers: the first time this database is set up, and — in Recreate mode — a
// destructive rebuild. The gate decides which by reading a __shiny_documentdb_aspire_seed marker row it
// keeps INSIDE the catalog database, so `docker volume rm sample-aspire-catalog` re-arms it and an
// ordinary AppHost restart does not.
static async Task SeedAsync(DocumentStoreSeedContext ctx, CancellationToken ct)
{
    var options = new DocumentStoreOptions
    {
        DatabaseProvider = new PostgreSqlDatabaseProvider(ctx.ConnectionString)
    };
    options.ConfigureCatalog();

    using var store = new DocumentStore(options);

    // false on a first-time setup — there is nothing to clear. Only Recreate mode ever sets it.
    if (ctx.Recreate && store is IDocumentMaintenance maintenance)
        await maintenance.ClearAll(ct);

    var applied = await DocumentSeedRunner.RunAsync(store, CatalogSeeders.All, cancellationToken: ct);
    Console.WriteLine($"[seed] {ctx.StoreName} ({ctx.Provider}) recreate={ctx.Recreate} applied=[{string.Join(", ", applied)}]");
}
