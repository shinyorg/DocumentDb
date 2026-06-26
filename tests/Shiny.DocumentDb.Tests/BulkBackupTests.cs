using System.Text;
using Shiny.DocumentDb.Sqlite;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests.Sqlite;

/// <summary>
/// Exercises the cross-provider streaming bulk surface (<see cref="IDocumentBackup"/>): Export → Restore
/// round-trips, each <see cref="BulkWriteMode"/>, chunking/commit, ClearExistingFirst, and the raw
/// <see cref="IDocumentBackup.BulkImportAsync"/> primitive over <see cref="RawDocument"/>.
/// </summary>
[Collection("SQLite")]
public class BulkBackupTests
{
    static DocumentStore NewStore() => new(new DocumentStoreOptions
    {
        DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:"),
        TableName = $"t{Guid.NewGuid():N}"
    });

    [Fact]
    public async Task Export_Then_Restore_RoundTrips()
    {
        using var source = NewStore();
        await source.BatchInsert(new[]
        {
            new User { Id = "u1", Name = "Alice", Age = 25 },
            new User { Id = "u2", Name = "Bob", Age = 30 }
        });
        await source.Insert(new Product { Id = "p1", Title = "Widget", Price = 9.99m });

        using var ms = new MemoryStream();
        await source.ExportAsync(ms);
        ms.Position = 0;

        using var target = NewStore();
        var result = await target.RestoreAsync(ms);

        Assert.Equal(3, result.DocumentsRead);
        Assert.Equal(3, result.DocumentsWritten);
        Assert.Equal(2, await target.Count<User>());
        Assert.Equal(1, await target.Count<Product>());
        Assert.Equal("Alice", (await target.Get<User>("u1"))!.Name);
        Assert.Equal("Widget", (await target.Get<Product>("p1"))!.Title);
    }

    [Fact]
    public async Task BulkImport_RawDocuments_Insert()
    {
        using var store = NewStore();
        var result = await store.BulkImportAsync(Rows(
            ("u1", "User", """{"id":"u1","name":"Alice","age":25}"""),
            ("u2", "User", """{"id":"u2","name":"Bob","age":30}""")));

        Assert.Equal(2, result.DocumentsWritten);
        Assert.Equal("Bob", (await store.Get<User>("u2"))!.Name);
    }

    [Fact]
    public async Task BulkImport_Replace_OverwritesBody()
    {
        using var store = NewStore();
        await store.Insert(new User { Id = "u1", Name = "Old", Age = 1 });

        var result = await store.BulkImportAsync(
            Rows(("u1", "User", """{"id":"u1","name":"New","age":99}""")),
            new BulkRestoreOptions { Mode = BulkWriteMode.Replace });

        Assert.Equal(1, result.DocumentsWritten);
        var u = await store.Get<User>("u1");
        Assert.Equal("New", u!.Name);
        Assert.Equal(99, u.Age);
    }

    [Fact]
    public async Task BulkImport_SkipExisting_LeavesExistingUntouched()
    {
        using var store = NewStore();
        await store.Insert(new User { Id = "u1", Name = "Original", Age = 1 });

        var result = await store.BulkImportAsync(
            Rows(
                ("u1", "User", """{"id":"u1","name":"ShouldBeSkipped","age":99}"""),
                ("u2", "User", """{"id":"u2","name":"Fresh","age":2}""")),
            new BulkRestoreOptions { Mode = BulkWriteMode.SkipExisting });

        Assert.Equal(1, result.DocumentsWritten);
        Assert.Equal(1, result.DocumentsSkipped);
        Assert.Equal("Original", (await store.Get<User>("u1"))!.Name);
        Assert.Equal("Fresh", (await store.Get<User>("u2"))!.Name);
    }

    [Fact]
    public async Task BulkImport_Merge_DeepMergesBody()
    {
        using var store = NewStore();
        await store.Insert(new User { Id = "u1", Name = "Alice", Age = 25 });

        await store.BulkImportAsync(
            Rows(("u1", "User", """{"age":26}""")),
            new BulkRestoreOptions { Mode = BulkWriteMode.Merge });

        var u = await store.Get<User>("u1");
        Assert.Equal("Alice", u!.Name);   // preserved
        Assert.Equal(26, u.Age);          // merged
    }

    [Fact]
    public async Task BulkImport_ClearExistingFirst_WipesThenWrites()
    {
        using var store = NewStore();
        await store.Insert(new User { Id = "old", Name = "Gone", Age = 1 });

        var result = await store.BulkImportAsync(
            Rows(("u1", "User", """{"id":"u1","name":"Alice","age":25}""")),
            new BulkRestoreOptions { ClearExistingFirst = true });

        Assert.Equal(1, result.DocumentsWritten);
        Assert.Null(await store.Get<User>("old"));
        Assert.Equal(1, await store.Count<User>());
    }

    [Fact]
    public async Task BulkImport_ChunkedPerChunkCommit_ReportsChunks()
    {
        using var store = NewStore();
        var rows = Enumerable.Range(0, 25)
            .Select(i => ($"u{i}", "User", $$"""{"id":"u{{i}}","name":"N{{i}}","age":{{i}}}"""))
            .ToArray();

        var result = await store.BulkImportAsync(Rows(rows), new BulkRestoreOptions { ChunkSize = 10 });

        Assert.Equal(25, result.DocumentsWritten);
        Assert.Equal(3, result.ChunksCommitted);   // 10 + 10 + 5
        Assert.Equal(25, await store.Count<User>());
    }

    [Fact]
    public async Task Export_RestoreInto_PopulatedStore_WithReplace()
    {
        using var source = NewStore();
        await source.Insert(new User { Id = "u1", Name = "FromBackup", Age = 25 });

        using var ms = new MemoryStream();
        await source.ExportAsync(ms);
        ms.Position = 0;

        using var target = NewStore();
        await target.Insert(new User { Id = "u1", Name = "Stale", Age = 1 });
        var result = await target.RestoreAsync(ms, new BulkRestoreOptions { Mode = BulkWriteMode.Replace });

        Assert.Equal(1, result.DocumentsWritten);
        Assert.Equal("FromBackup", (await target.Get<User>("u1"))!.Name);
    }

    static async IAsyncEnumerable<RawDocument> Rows(params (string id, string docType, string json)[] rows)
    {
        foreach (var (id, docType, json) in rows)
        {
            yield return new RawDocument(id, docType, Encoding.UTF8.GetBytes(json));
            await Task.CompletedTask;
        }
    }
}

/// <summary>
/// Runtime smoke of the bulk backup surface against DuckDB — verifies the provider's CAST(... AS JSON) +
/// ON CONFLICT overrides for Replace / SkipExisting / Merge actually execute (SQL shape is asserted in the
/// SQLite suite).
/// </summary>
[Collection("DuckDB")]
public class BulkBackupDuckDbTests(Fixtures.DuckDbDatabaseFixture db)
{
    readonly Fixtures.DuckDbDatabaseFixture db = db;

    [Fact]
    public async Task Export_Then_Restore_RoundTrips()
    {
        using var source = (IDisposable)this.db.CreateStore($"t{Guid.NewGuid():N}");
        var src = (IDocumentBackup)source;
        await ((IDocumentStore)source).BatchInsert(new[]
        {
            new User { Id = "u1", Name = "Alice", Age = 25 },
            new User { Id = "u2", Name = "Bob", Age = 30 }
        });

        using var ms = new MemoryStream();
        await src.ExportAsync(ms);
        ms.Position = 0;

        using var target = (IDisposable)this.db.CreateStore($"t{Guid.NewGuid():N}");
        var result = await ((IDocumentBackup)target).RestoreAsync(ms);

        Assert.Equal(2, result.DocumentsWritten);
        Assert.Equal("Alice", (await ((IDocumentStore)target).Get<User>("u1"))!.Name);
    }

    [Fact]
    public async Task BulkImport_AllModes()
    {
        using var store = (IDisposable)this.db.CreateStore($"t{Guid.NewGuid():N}");
        var s = (IDocumentStore)store;
        var b = (IDocumentBackup)store;

        await b.BulkImportAsync(Rows(("u1", "User", """{"id":"u1","name":"A","age":1}""")));
        Assert.Equal("A", (await s.Get<User>("u1"))!.Name);

        await b.BulkImportAsync(Rows(("u1", "User", """{"id":"u1","name":"B","age":2}""")),
            new BulkRestoreOptions { Mode = BulkWriteMode.Replace });
        Assert.Equal("B", (await s.Get<User>("u1"))!.Name);

        await b.BulkImportAsync(Rows(("u1", "User", """{"id":"u1","name":"SKIP","age":9}""")),
            new BulkRestoreOptions { Mode = BulkWriteMode.SkipExisting });
        Assert.Equal("B", (await s.Get<User>("u1"))!.Name);

        await b.BulkImportAsync(Rows(("u1", "User", """{"age":42}""")),
            new BulkRestoreOptions { Mode = BulkWriteMode.Merge });
        var u = await s.Get<User>("u1");
        Assert.Equal("B", u!.Name);
        Assert.Equal(42, u.Age);
    }

    static async IAsyncEnumerable<RawDocument> Rows(params (string id, string docType, string json)[] rows)
    {
        foreach (var (id, docType, json) in rows)
        {
            yield return new RawDocument(id, docType, Encoding.UTF8.GetBytes(json));
            await Task.CompletedTask;
        }
    }
}
