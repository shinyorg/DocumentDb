using Shiny.DocumentDb.Sqlite;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests.Sqlite;

/// <summary>
/// Exercises <see cref="IDocumentStore.BatchUpsert"/>, <see cref="IDocumentStore.BatchUpdate"/> and
/// <see cref="IDocumentStore.BatchRemove"/>: the relational fast paths (single multi-row upsert /
/// delete-by-id-list), the all-or-nothing fallback for versioned types, and the UnitOfWork coalescing
/// of contiguous same-kind runs. SQL is captured via the store's logger to assert the fast-path shape.
/// </summary>
[Collection("SQLite")]
public class BatchWriteTests(SqliteDatabaseFixture db)
{
    readonly SqliteDatabaseFixture db = db;

    static DocumentStore NewStore(out List<string> sql)
    {
        var captured = new List<string>();
        sql = captured;
        return new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:"),
            TableName = $"t{Guid.NewGuid():N}",
            Logging = captured.Add
        });
    }

    // ── BatchUpsert ─────────────────────────────────────────────────────

    [Fact]
    public async Task BatchUpsert_InsertsNew_AndUpdatesExisting()
    {
        using var store = NewStore(out _);
        await store.BatchUpsert(new[]
        {
            new User { Id = "a", Name = "A", Age = 1 },
            new User { Id = "b", Name = "B", Age = 2 }
        });
        Assert.Equal(2, await store.Count<User>());

        await store.BatchUpsert(new[]
        {
            new User { Id = "a", Name = "A2", Age = 10 }, // updates existing
            new User { Id = "c", Name = "C", Age = 3 }    // inserts new
        });

        Assert.Equal(3, await store.Count<User>());
        Assert.Equal("A2", (await store.Get<User>("a"))!.Name);
        Assert.Equal("C", (await store.Get<User>("c"))!.Name);
    }

    [Fact]
    public async Task BatchUpsert_EmptyCollection_ReturnsZero()
    {
        using var store = NewStore(out _);
        Assert.Equal(0, await store.BatchUpsert(Array.Empty<User>()));
    }

    [Fact]
    public async Task BatchUpsert_FastPath_EmitsSingleMultiRowStatement()
    {
        using var store = NewStore(out var sql);
        await store.BatchUpsert(new[]
        {
            new User { Id = "a", Name = "A", Age = 1 },
            new User { Id = "b", Name = "B", Age = 2 },
            new User { Id = "c", Name = "C", Age = 3 }
        });

        var upserts = sql.Where(s => s.Contains("ON CONFLICT")).ToList();
        Assert.Single(upserts);                       // one statement for the whole batch
        Assert.Contains("json_patch", upserts[0]);    // SQLite deep-merge fast path
        Assert.Contains("@data_2", upserts[0]);       // all three rows in the one statement
    }

    // ── BatchUpdate ─────────────────────────────────────────────────────

    [Fact]
    public async Task BatchUpdate_UpdatesAll()
    {
        using var store = NewStore(out _);
        await store.BatchInsert(new[]
        {
            new User { Id = "a", Name = "A", Age = 1 },
            new User { Id = "b", Name = "B", Age = 2 }
        });

        var n = await store.BatchUpdate(new[]
        {
            new User { Id = "a", Name = "A!", Age = 11 },
            new User { Id = "b", Name = "B!", Age = 22 }
        });

        Assert.Equal(2, n);
        Assert.Equal("A!", (await store.Get<User>("a"))!.Name);
        Assert.Equal(22, (await store.Get<User>("b"))!.Age);
    }

    [Fact]
    public async Task BatchUpdate_MissingDocument_RollsBackWholeBatch()
    {
        using var store = NewStore(out _);
        await store.BatchInsert(new[]
        {
            new User { Id = "a", Name = "A", Age = 1 },
            new User { Id = "b", Name = "B", Age = 2 }
        });

        // 'missing' does not exist — the whole batch must roll back, leaving a and b untouched.
        await Assert.ThrowsAsync<InvalidOperationException>(() => store.BatchUpdate(new[]
        {
            new User { Id = "a", Name = "A!", Age = 11 },
            new User { Id = "b", Name = "B!", Age = 22 },
            new User { Id = "missing", Name = "X", Age = 9 }
        }));

        Assert.Equal("A", (await store.Get<User>("a"))!.Name);
        Assert.Equal(2, (await store.Get<User>("b"))!.Age);
    }

    // ── BatchRemove ─────────────────────────────────────────────────────

    [Fact]
    public async Task BatchRemove_DeletesExisting_IgnoresMissing_ReturnsCount()
    {
        using var store = NewStore(out var sql);
        await store.BatchInsert(new[]
        {
            new User { Id = "a", Name = "A", Age = 1 },
            new User { Id = "b", Name = "B", Age = 2 },
            new User { Id = "c", Name = "C", Age = 3 }
        });

        var removed = await store.BatchRemove<User>(new object[] { "a", "c", "missing" });

        Assert.Equal(2, removed);                       // 'missing' ignored
        Assert.Equal(1, await store.Count<User>());
        Assert.NotNull(await store.Get<User>("b"));

        var deletes = sql.Where(s => s.Contains("DELETE FROM") && s.Contains("IN (")).ToList();
        Assert.Single(deletes);                         // one delete-by-id-list statement
    }

    [Fact]
    public async Task BatchRemove_EmptyCollection_ReturnsZero()
    {
        using var store = NewStore(out _);
        Assert.Equal(0, await store.BatchRemove<User>(Array.Empty<object>()));
    }

    // ── UnitOfWork coalescing ───────────────────────────────────────────

    [Fact]
    public async Task UnitOfWork_CoalescesContiguousUpsertAndRemoveRuns()
    {
        using var store = NewStore(out var sql);
        await store.BatchInsert(new[]
        {
            new User { Id = "x", Name = "X", Age = 1 },
            new User { Id = "y", Name = "Y", Age = 2 }
        });
        sql.Clear();

        await store.CreateUnitOfWork()
            .Upsert(new User { Id = "a", Name = "A", Age = 1 })
            .Upsert(new User { Id = "b", Name = "B", Age = 2 })
            .Upsert(new User { Id = "c", Name = "C", Age = 3 })
            .Remove<User>("x")
            .Remove<User>("y")
            .SaveChanges();

        // Three contiguous upserts coalesce into one multi-row statement; two removes into one delete.
        Assert.Single(sql, s => s.Contains("ON CONFLICT"));
        Assert.Single(sql, s => s.Contains("DELETE FROM") && s.Contains("IN ("));
        Assert.Equal(3, await store.Count<User>());
    }

    // ── Versioned all-or-nothing (fallback path) ────────────────────────

    [Fact]
    public async Task BatchUpdate_Versioned_StaleVersion_RollsBackWholeBatch()
    {
        using var store = (IDisposable)this.db.CreateStoreWithVersion<VersionedUser>(
            $"t{Guid.NewGuid():N}", u => u.Version);
        var s = (IDocumentStore)store;

        await s.Insert(new VersionedUser { Id = "u1", Name = "Alice", Age = 30 });
        await s.Insert(new VersionedUser { Id = "u2", Name = "Bob", Age = 40 });

        // Snapshot both at version 1, then bump u1 out-of-band so the batch's copy is stale.
        var c1 = await s.Get<VersionedUser>("u1");
        var c2 = await s.Get<VersionedUser>("u2");
        await s.Update(new VersionedUser { Id = "u1", Name = "Alice2", Age = 31, Version = 1 });

        c1!.Age = 99;
        c2!.Age = 88;
        await Assert.ThrowsAsync<ConcurrencyException>(() => s.BatchUpdate(new[] { c1, c2 }));

        // u2's change must not have persisted — the whole batch rolled back.
        Assert.Equal(40, (await s.Get<VersionedUser>("u2"))!.Age);
    }

    // ── Temporal sidecar (fallback path) ────────────────────────────────

    [Fact]
    public async Task BatchRemove_Temporal_RecordsTombstones()
    {
        using var store = (IDisposable)this.db.CreateTemporalStore($"t{Guid.NewGuid():N}");
        var s = (IDocumentStore)store;
        var temporal = (ITemporalDocumentStore)store;

        await s.Insert(new VersionedUser { Id = "u1", Name = "Alice", Age = 30 });
        await s.Insert(new VersionedUser { Id = "u2", Name = "Bob", Age = 40 });

        var removed = await s.BatchRemove<VersionedUser>(new object[] { "u1", "u2" });

        Assert.Equal(2, removed);
        Assert.Equal(0, await s.Count<VersionedUser>());

        // History retains the lifecycle of a removed document (Inserted then Removed).
        var history = await temporal.History<VersionedUser>("u1");
        Assert.Contains(history, h => h.Operation == TemporalOperation.Removed);
    }
}
