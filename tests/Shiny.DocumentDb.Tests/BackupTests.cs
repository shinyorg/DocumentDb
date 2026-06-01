using Shiny.DocumentDb.Tests.Fixtures;
using Shiny.DocumentDb.Sqlite;
using Xunit;

namespace Shiny.DocumentDb.Tests;

public class BackupTests : IDisposable
{
    readonly SqliteDocumentStore store;
    readonly string sourcePath;
    readonly string backupPath;

    public BackupTests()
    {
        // Backup() opens a fresh SqliteConnection to the source connection string. Using a
        // file-based source rather than `:memory:` ensures both the store's connection and the
        // backup's connection see the same database. (`:memory:` is per-connection — each open
        // returns a private blank DB.)
        this.sourcePath = Path.Combine(Path.GetTempPath(), $"src_{Guid.NewGuid():N}.db");
        this.store = new SqliteDocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider($"Data Source={this.sourcePath}")
        });
        this.backupPath = Path.Combine(Path.GetTempPath(), $"backup_{Guid.NewGuid():N}.db");
    }

    public void Dispose()
    {
        this.store.Dispose();
        if (File.Exists(this.backupPath))
            File.Delete(this.backupPath);
        if (File.Exists(this.sourcePath))
            File.Delete(this.sourcePath);
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

}
