using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests.DuckDb;

/// <summary>
/// Smoke-tests the batch write methods against DuckDB, whose native multi-row upsert SQL differs from
/// SQLite (<c>json_merge_patch</c>, <c>CAST(... AS JSON)</c>, <c>EXCLUDED</c>). Behavioural only — the
/// SQL-shape assertions live in the SQLite <see cref="Sqlite.BatchWriteTests"/>.
/// </summary>
[Collection("DuckDB")]
public class BatchWriteDuckDbTests(DuckDbDatabaseFixture db)
{
    readonly DuckDbDatabaseFixture db = db;

    [Fact]
    public async Task BatchUpsert_InsertsNew_AndUpdatesExisting()
    {
        using var store = (IDisposable)this.db.CreateStore($"t{Guid.NewGuid():N}");
        var s = (IDocumentStore)store;

        await s.BatchUpsert(new[]
        {
            new User { Id = "a", Name = "A", Age = 1 },
            new User { Id = "b", Name = "B", Age = 2 }
        });
        Assert.Equal(2, await s.Count<User>());

        await s.BatchUpsert(new[]
        {
            new User { Id = "a", Name = "A2", Age = 10 },
            new User { Id = "c", Name = "C", Age = 3 }
        });

        Assert.Equal(3, await s.Count<User>());
        Assert.Equal("A2", (await s.Get<User>("a"))!.Name);
    }

    [Fact]
    public async Task BatchUpdate_UpdatesAll()
    {
        using var store = (IDisposable)this.db.CreateStore($"t{Guid.NewGuid():N}");
        var s = (IDocumentStore)store;

        await s.BatchInsert(new[]
        {
            new User { Id = "a", Name = "A", Age = 1 },
            new User { Id = "b", Name = "B", Age = 2 }
        });
        await s.BatchUpdate(new[]
        {
            new User { Id = "a", Name = "A!", Age = 11 },
            new User { Id = "b", Name = "B!", Age = 22 }
        });

        Assert.Equal("A!", (await s.Get<User>("a"))!.Name);
        Assert.Equal(22, (await s.Get<User>("b"))!.Age);
    }

    [Fact]
    public async Task BatchRemove_DeletesExisting_IgnoresMissing_ReturnsCount()
    {
        using var store = (IDisposable)this.db.CreateStore($"t{Guid.NewGuid():N}");
        var s = (IDocumentStore)store;

        await s.BatchInsert(new[]
        {
            new User { Id = "a", Name = "A", Age = 1 },
            new User { Id = "b", Name = "B", Age = 2 },
            new User { Id = "c", Name = "C", Age = 3 }
        });

        Assert.Equal(2, await s.BatchRemove<User>(new object[] { "a", "c", "missing" }));
        Assert.Equal(1, await s.Count<User>());
    }
}
