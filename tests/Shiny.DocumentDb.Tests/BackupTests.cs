using Shiny.DocumentDb.Tests.Fixtures;
using Shiny.DocumentDb.Sqlite;
using Xunit;

namespace Shiny.DocumentDb.Tests;

public class BackupTests : IDisposable
{
    readonly SqliteDocumentStore store;
    readonly string backupPath;

    public BackupTests()
    {
        this.store = new SqliteDocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:")
        });
        this.backupPath = Path.Combine(Path.GetTempPath(), $"backup_{Guid.NewGuid():N}.db");
    }

    public void Dispose()
    {
        this.store.Dispose();
        if (File.Exists(this.backupPath))
            File.Delete(this.backupPath);
    }

    [Fact]
    public async Task Backup_CreatesFileWithAllData()
    {
        await this.store.Insert(new User { Id = "u1", Name = "Alice", Age = 25 });
        await this.store.Insert(new User { Id = "u2", Name = "Bob", Age = 30 });
        await this.store.Insert(new Product { Id = "p1", Title = "Widget", Price = 9.99m });

        await this.store.Backup(this.backupPath);

        Assert.True(File.Exists(this.backupPath));

        using var restored = new SqliteDocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider($"Data Source={this.backupPath}")
        });
        var users = await restored.Query<User>().ToList();
        var products = await restored.Query<Product>().ToList();

        Assert.Equal(2, users.Count);
        Assert.Single(products);
        Assert.Contains(users, u => u.Name == "Alice");
        Assert.Contains(users, u => u.Name == "Bob");
        Assert.Equal("Widget", products[0].Title);
    }

    [Fact]
    public async Task Backup_ThrowsOnNullOrEmptyPath()
    {
        await Assert.ThrowsAnyAsync<ArgumentException>(() => this.store.Backup(null!));
        await Assert.ThrowsAnyAsync<ArgumentException>(() => this.store.Backup(""));
        await Assert.ThrowsAnyAsync<ArgumentException>(() => this.store.Backup("   "));
    }

    [Fact]
    public async Task Backup_ThrowsInsideTransaction()
    {
        await this.store.Insert(new User { Id = "u1", Name = "Alice" });

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await this.store.RunInTransaction(async tx =>
            {
                await tx.Backup(this.backupPath);
            });
        });
    }
}
