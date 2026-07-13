using System.Linq.Expressions;
using Raven.Client.Documents;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Shiny.DocumentDb.RavenDb;
using Testcontainers.RavenDb;
using Xunit;

namespace Shiny.DocumentDb.Tests.Fixtures;

/// <summary>
/// Spins up a RavenDB server in a container for the conformance suite. Each store gets its own RavenDB
/// database (named by the supplied <c>tableName</c>) for full isolation between tests.
/// </summary>
public class RavenDbDatabaseFixture : IDocumentStoreFixture, IAsyncLifetime
{
    RavenDbContainer container = null!;
    string url = null!;
    global::Raven.Client.Documents.IDocumentStore bootstrap = null!;

    RavenDbDocumentStoreOptions BaseOptions(string database)
    {
        this.EnsureDatabase(database);
        return new RavenDbDocumentStoreOptions
        {
            Urls = [this.url],
            Database = database
        };
    }

    void EnsureDatabase(string database)
    {
        try
        {
            this.bootstrap.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(database)));
        }
        catch (ConcurrencyException)
        {
            // Database already exists — fine.
        }
    }

    public IDocumentStore CreateStore(string tableName)
        => new RavenDbDocumentStore(this.BaseOptions(tableName));

    public IDocumentStore CreateStoreWithFilter<T>(string tableName, Expression<Func<T, bool>> filter) where T : class
    {
        var opts = this.BaseOptions(tableName);
        opts.AddQueryFilter(filter);
        return new RavenDbDocumentStore(opts);
    }

    public IDocumentStore CreateStoreWithNamedFilter<T>(string tableName, string filterName, Expression<Func<T, bool>> filter) where T : class
    {
        var opts = this.BaseOptions(tableName);
        opts.AddQueryFilter(filterName, filter);
        return new RavenDbDocumentStore(opts);
    }

    public IDocumentStore CreateStoreWithVersion<T>(string tableName, Expression<Func<T, int>> versionProperty) where T : class
    {
        var opts = this.BaseOptions(tableName);
        opts.MapVersionProperty(versionProperty);
        return new RavenDbDocumentStore(opts);
    }

    public async ValueTask InitializeAsync()
    {
        // The Testcontainers module defaults to a RavenDB 5.4 server image, which can't parse the
        // 7.2 client's requests — pin the server to match the RavenDB.Client major/minor.
        this.container = new RavenDbBuilder()
            .WithImage("ravendb/ravendb:7.2-ubuntu-latest")
            .Build();
        await this.container.StartAsync();
        this.url = this.container.GetConnectionString();

        var store = new global::Raven.Client.Documents.DocumentStore { Urls = [this.url] };
        store.Initialize();
        this.bootstrap = store;
    }

    public async ValueTask DisposeAsync()
    {
        this.bootstrap?.Dispose();
        await this.container.DisposeAsync();
    }
}
