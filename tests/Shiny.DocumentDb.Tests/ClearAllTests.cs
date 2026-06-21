using Shiny.DocumentDb.Sqlite;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

public class ClearAllTests : IDisposable
{
    readonly SqliteDocumentStore store;
    readonly string path;

    public ClearAllTests()
    {
        this.path = Path.Combine(Path.GetTempPath(), $"clearall_{Guid.NewGuid():N}.db");
        this.store = new SqliteDocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider($"Data Source={this.path}")
        });
    }

    public void Dispose()
    {
        this.store.Dispose();
        if (File.Exists(this.path))
            File.Delete(this.path);
    }

    [Fact]
    public async Task ClearsEveryType()
    {
        await this.store.Insert(new User { Id = "u1", Name = "Alice", Age = 25 });
        await this.store.Insert(new User { Id = "u2", Name = "Bob", Age = 30 });
        await this.store.Insert(new Product { Id = "p1", Title = "Widget", Price = 9.99m });

        await ((IDocumentMaintenance)this.store).ClearAll();

        Assert.Equal(0, await this.store.Count<User>());
        Assert.Equal(0, await this.store.Count<Product>());
    }

    [Fact]
    public void StoreImplementsMaintenance()
        => Assert.IsAssignableFrom<IDocumentMaintenance>(this.store);

    [Fact]
    public async Task ClearAllAsyncDelegatesToClearAll()
    {
        await this.store.Insert(new User { Id = "u1", Name = "Alice", Age = 25 });

        await this.store.ClearAllAsync();

        Assert.Equal(0, await this.store.Count<User>());
    }

    [Fact]
    public async Task StoreUsableAfterClearAll()
    {
        await this.store.Insert(new User { Id = "u1", Name = "Alice", Age = 25 });
        await ((IDocumentMaintenance)this.store).ClearAll();

        await this.store.Insert(new User { Id = "u2", Name = "Bob", Age = 30 });
        Assert.Equal(1, await this.store.Count<User>());
    }
}
