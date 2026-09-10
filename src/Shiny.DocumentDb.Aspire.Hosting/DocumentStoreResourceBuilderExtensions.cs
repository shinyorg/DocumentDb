using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Shiny.DocumentDb.Aspire.Hosting.Internal;

namespace Shiny.DocumentDb.Aspire.Hosting;

/// <summary>
/// AppHost-side extensions for declaring a DocumentDb store on top of an Aspire database resource.
/// v1 covers the relational providers + SQLite (Postgres, SQL Server, MySQL, SQLite).
/// </summary>
// TODO: Mongo/Cosmos follow-up — add UseMongo()/UseCosmos() composites once the Aspire.Hosting.MongoDB
// and Azure Cosmos hosting packages are pinned and the client supports their divergent registration.
public static class DocumentStoreResourceBuilderExtensions
{
    /// <summary>
    /// Layers a DocumentDb store onto an existing database resource. When <paramref name="kind"/> is
    /// <c>null</c> the provider is auto-detected from the resource type (Postgres/SqlServer/MySql/Sqlite).
    /// </summary>
    public static IResourceBuilder<DocumentStoreResource> AsDocumentStore(
        this IResourceBuilder<IResourceWithConnectionString> db,
        string name,
        DocumentProviderKind? kind = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        if (string.Equals(name, db.Resource.Name, StringComparison.OrdinalIgnoreCase))
            throw new ArgumentException(
                $"The DocumentDb store name '{name}' must differ from the backing resource name '{db.Resource.Name}'. " +
                "Pass a distinct store name to AsDocumentStore (the store name becomes the consumer-facing resource).",
                nameof(name));

        var resolvedKind = kind ?? DocumentProviderKindDetector.Detect(db);
        var resource = new DocumentStoreResource(name, db.Resource, resolvedKind);

        return db.ApplicationBuilder
            .AddResource(resource)
            .WithParentRelationship(db.Resource)
            .WithInitialState(new()
            {
                ResourceType = "DocumentStore",
                State = new("Running", KnownResourceStateStyles.Success),
                Properties =
                [
                    new("documentdb.provider", resolvedKind.ToString()),
                    new("documentdb.backing", db.Resource.Name)
                ]
            });
    }

    /// <summary>
    /// Provisions a PostgreSQL server + database and exposes it as a DocumentDb store named
    /// <paramref name="name"/>.
    /// </summary>
    public static IResourceBuilder<DocumentStoreResource> AddPostgresDocumentStore(
        this IDistributedApplicationBuilder builder,
        string name)
    {
        var db = builder
            .AddPostgres($"{name}-server")
            .AddDatabase($"{name}-db");

        return ((IResourceBuilder<IResourceWithConnectionString>)db).AsDocumentStore(name, DocumentProviderKind.Postgres);
    }

    /// <summary>
    /// Provisions a SQL Server server + database and exposes it as a DocumentDb store named
    /// <paramref name="name"/>.
    /// </summary>
    public static IResourceBuilder<DocumentStoreResource> AddSqlServerDocumentStore(
        this IDistributedApplicationBuilder builder,
        string name)
    {
        var db = builder
            .AddSqlServer($"{name}-server")
            .AddDatabase($"{name}-db");

        return ((IResourceBuilder<IResourceWithConnectionString>)db).AsDocumentStore(name, DocumentProviderKind.SqlServer);
    }

    /// <summary>
    /// Provisions a MySQL server + database and exposes it as a DocumentDb store named
    /// <paramref name="name"/>.
    /// </summary>
    public static IResourceBuilder<DocumentStoreResource> AddMySqlDocumentStore(
        this IDistributedApplicationBuilder builder,
        string name)
    {
        var db = builder
            .AddMySql($"{name}-server")
            .AddDatabase($"{name}-db");

        return ((IResourceBuilder<IResourceWithConnectionString>)db).AsDocumentStore(name, DocumentProviderKind.MySql);
    }

    /// <summary>
    /// Exposes a file-backed SQLite database as a DocumentDb store named <paramref name="name"/>.
    /// SQLite has no container, so this models the connection string directly (the file is "ready"
    /// immediately — no <c>WaitFor</c> on a service).
    /// </summary>
    public static IResourceBuilder<DocumentStoreResource> AddSqliteDocumentStore(
        this IDistributedApplicationBuilder builder,
        string name,
        string filePath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(filePath);

        var cs = builder.AddConnectionString(
            $"{name}-sqlite",
            ReferenceExpression.Create($"Data Source={filePath}"));

        return cs.AsDocumentStore(name, DocumentProviderKind.Sqlite);
    }

    /// <summary>
    /// References a DocumentDb store from a consuming resource: injects the connection string
    /// (<c>ConnectionStrings:&lt;name&gt;</c>) AND the provider discriminator
    /// (<c>Shiny__DocumentDb__&lt;name&gt;__Provider</c>) the client integration reads.
    /// </summary>
    public static IResourceBuilder<TDestination> WithReference<TDestination>(
        this IResourceBuilder<TDestination> builder,
        IResourceBuilder<DocumentStoreResource> store)
        where TDestination : IResourceWithEnvironment
    {
        var name = store.Resource.Name;
        var kind = store.Resource.Kind;

        return builder
            .WithReference((IResourceBuilder<IResourceWithConnectionString>)store)
            .WithEnvironment(DocumentStoreConstants.ProviderEnvVar(name), kind.ToString());
    }

    /// <summary>
    /// Registers a seed step that runs on exactly two triggers: the <b>first time the database is set
    /// up</b>, and — when <paramref name="mode"/> is <see cref="DocumentStoreSeedMode.Recreate"/> — a
    /// <b>destructive recreation</b>. Steady-state AppHost restarts do not re-seed.
    /// </summary>
    /// <remarks>
    /// The "already seeded" marker is written into the backing database itself, so wiping the data
    /// (dropping the container volume, deleting the SQLite file) also wipes the marker and the next start
    /// counts as a first-time setup. The callback receives the store's resolved connection string plus
    /// <see cref="DocumentStoreSeedContext.Recreate"/>, which tells it whether it is filling an empty
    /// database or rebuilding a populated one — on a rebuild the callback owns the wipe (it holds the
    /// store, so it can call <c>IDocumentMaintenance.ClearAll</c>); the AppHost issues no DDL beyond
    /// its own marker table.
    /// </remarks>
    public static IResourceBuilder<DocumentStoreResource> WithSeeder(
        this IResourceBuilder<DocumentStoreResource> store,
        Func<DocumentStoreSeedContext, CancellationToken, Task> seed,
        DocumentStoreSeedMode mode = DocumentStoreSeedMode.FirstTimeOnly)
    {
        ArgumentNullException.ThrowIfNull(seed);

        var storeResource = store.Resource;
        var backing = storeResource.Backing;

        store.ApplicationBuilder.Eventing.Subscribe<ResourceReadyEvent>(
            backing,
            async (@event, ct) =>
            {
                var logger = @event.Services.GetRequiredService<ILoggerFactory>()
                    .CreateLogger("Shiny.DocumentDb.Aspire.Seeder");

                var connectionString = await backing.GetConnectionStringAsync(ct);
                if (string.IsNullOrEmpty(connectionString))
                    throw new InvalidOperationException(
                        $"Connection string for DocumentDb store '{storeResource.Name}' was null or empty.");

                await DocumentStoreSeedGate.RunAsync(
                    storeResource.Name,
                    storeResource.Kind,
                    connectionString,
                    mode,
                    seed,
                    logger,
                    ct
                );
            });

        return store;
    }
}
