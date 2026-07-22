using System.Linq.Expressions;
using Shiny.DocumentDb.Redis;
using Testcontainers.Redis;
using Xunit;

namespace Shiny.DocumentDb.Tests.Fixtures;

public class RedisDatabaseFixture : IDocumentStoreFixture, IAsyncLifetime
{
    RedisContainer container = null!;
    string connectionString = null!;

    RedisDocumentStoreOptions BaseOptions(string tableName) => new()
    {
        ConnectionString = this.connectionString,
        // isolate each test's documents/indexes under a per-store key namespace
        KeyNamespace = tableName
    };

    public IDocumentStore CreateStore(string tableName)
        => new RedisDocumentStore(this.BaseOptions(tableName));

    /// <summary>Builds a store with caller-supplied option tweaks (e.g. blob mappings).</summary>
    public RedisDocumentStore CreateConfiguredStore(string tableName, Action<RedisDocumentStoreOptions> configure)
    {
        var opts = this.BaseOptions(tableName);
        configure(opts);
        return new RedisDocumentStore(opts);
    }

    public IDocumentStore CreateStoreWithFilter<T>(string tableName, Expression<Func<T, bool>> filter) where T : class
    {
        var opts = this.BaseOptions(tableName);
        opts.AddQueryFilter(filter);
        return new RedisDocumentStore(opts);
    }

    public IDocumentStore CreateStoreWithNamedFilter<T>(string tableName, string filterName, Expression<Func<T, bool>> filter) where T : class
    {
        var opts = this.BaseOptions(tableName);
        opts.AddQueryFilter(filterName, filter);
        return new RedisDocumentStore(opts);
    }

    public IDocumentStore CreateStoreWithVersion<T>(string tableName, Expression<Func<T, int>> versionProperty) where T : class
    {
        var opts = this.BaseOptions(tableName);
        opts.MapVersionProperty(versionProperty);
        return new RedisDocumentStore(opts);
    }

    public IDocumentStore CreateStoreWithIndexed<T>(string tableName, Expression<Func<T, object>> indexedProperty) where T : class
    {
        var opts = this.BaseOptions(tableName);
        opts.MapIndexedProperty(indexedProperty);
        return new RedisDocumentStore(opts);
    }

    /// <summary>A store whose <c>User.Name</c> (TAG) and <c>User.Age</c> (NUMERIC) fields are indexed so the
    /// server-side <c>FT.AGGREGATE</c> GroupBy path is reachable.</summary>
    public IDocumentStore CreateGroupByStore(string tableName)
    {
        var opts = this.BaseOptions(tableName);
        opts.MapIndexedProperty<User>(u => u.Name);
        opts.MapIndexedProperty<User>(u => u.Age);
        return new RedisDocumentStore(opts);
    }

    public async ValueTask InitializeAsync()
    {
        // redis-stack-server bundles the RediSearch + RedisJSON modules the provider requires.
        this.container = new RedisBuilder()
            .WithImage("redis/redis-stack-server:latest")
            .Build();
        await this.container.StartAsync();
        this.connectionString = this.container.GetConnectionString();
    }

    public async ValueTask DisposeAsync() => await this.container.DisposeAsync();
}
