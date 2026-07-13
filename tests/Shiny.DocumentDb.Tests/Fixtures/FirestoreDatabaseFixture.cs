using System.Linq.Expressions;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Google.Cloud.Firestore;
using Shiny.DocumentDb.Firestore;
using Xunit;

namespace Shiny.DocumentDb.Tests.Fixtures;

/// <summary>
/// Spins up the Google Cloud Firestore emulator in a generic Testcontainers container (there is no dedicated
/// Testcontainers.Firestore module) and points a shared <see cref="FirestoreDb"/> at it via the
/// <c>FIRESTORE_EMULATOR_HOST</c> environment variable. Each created store uses a per-test collection prefix so
/// the conformance suites stay isolated inside the single emulator database.
/// </summary>
public class FirestoreDatabaseFixture : IDocumentStoreFixture, IAsyncLifetime
{
    const string ProjectId = "shiny-documentdb-test";
    const int EmulatorPort = 8080;

    IContainer container = null!;
    FirestoreDb db = null!;

    FirestoreDocumentStoreOptions BaseOptions(string tableName) => new()
    {
        FirestoreDb = this.db,
        CollectionPrefix = tableName
    };

    public IDocumentStore CreateStore(string tableName)
        => new FirestoreDocumentStore(this.BaseOptions(tableName));

    public IDocumentStore CreateStoreWithFilter<T>(string tableName, Expression<Func<T, bool>> filter) where T : class
    {
        var opts = this.BaseOptions(tableName);
        opts.AddQueryFilter(filter);
        return new FirestoreDocumentStore(opts);
    }

    public IDocumentStore CreateStoreWithNamedFilter<T>(string tableName, string filterName, Expression<Func<T, bool>> filter) where T : class
    {
        var opts = this.BaseOptions(tableName);
        opts.AddQueryFilter(filterName, filter);
        return new FirestoreDocumentStore(opts);
    }

    public IDocumentStore CreateStoreWithVersion<T>(string tableName, Expression<Func<T, int>> versionProperty) where T : class
    {
        var opts = this.BaseOptions(tableName);
        opts.MapVersionProperty(versionProperty);
        return new FirestoreDocumentStore(opts);
    }

    public async ValueTask InitializeAsync()
    {
        this.container = new ContainerBuilder()
            .WithImage("gcr.io/google.com/cloudsdktool/google-cloud-cli:latest")
            .WithCommand("gcloud", "emulators", "firestore", "start", $"--host-port=0.0.0.0:{EmulatorPort}")
            .WithPortBinding(EmulatorPort, true)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("running"))
            .Build();
        await this.container.StartAsync();

        var host = $"{this.container.Hostname}:{this.container.GetMappedPublicPort(EmulatorPort)}";
        Environment.SetEnvironmentVariable("FIRESTORE_EMULATOR_HOST", host);

        this.db = new FirestoreDbBuilder
        {
            ProjectId = ProjectId,
            EmulatorDetection = Google.Api.Gax.EmulatorDetection.EmulatorOnly
        }.Build();
    }

    public async ValueTask DisposeAsync() => await this.container.DisposeAsync();
}
