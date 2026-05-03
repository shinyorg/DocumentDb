using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

public abstract class TableMappingTestsBase : IDisposable
{
    protected readonly IDatabaseFixture Fixture;
    protected readonly IDocumentStore store;

    protected TableMappingTestsBase(IDatabaseFixture fixture)
    {
        this.Fixture = fixture;
        this.store = new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = fixture.CreateProvider(),
            TableName = $"t{Guid.NewGuid():N}"
        });
    }

    public void Dispose() => (this.store as IDisposable)?.Dispose();

    [Fact]
    public async Task DefaultTableName_CanBeCustomized()
    {
        using var store = new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = Fixture.CreateProvider(),
            TableName = $"my_docs_{Guid.NewGuid():N}"
        });

        await store.Insert(new User { Id = "1", Name = "Alice", Age = 30, Email = "a@test.com" });
        var user = await store.Get<User>("1");
        Assert.NotNull(user);
        Assert.Equal("Alice", user.Name);
    }

    [Fact]
    public async Task MapTypeToTable_AutoDerivedName()
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = Fixture.CreateProvider()
        };
        opts.MapTypeToTable<User>();

        using var store = new DocumentStore(opts);

        await store.Insert(new User { Id = "1", Name = "Alice", Age = 30, Email = "a@test.com" });
        var user = await store.Get<User>("1");
        Assert.NotNull(user);
        Assert.Equal("Alice", user.Name);
    }

    [Fact]
    public async Task MapTypeToTable_ExplicitName()
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = Fixture.CreateProvider()
        };
        opts.MapTypeToTable<User>($"users_{Guid.NewGuid():N}");

        using var store = new DocumentStore(opts);

        await store.Insert(new User { Id = "1", Name = "Bob", Age = 25, Email = "b@test.com" });
        var count = await store.Count<User>();
        Assert.Equal(1, count);
    }

    [Fact]
    public async Task MappedAndUnmappedTypes_UseCorrectTables()
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = Fixture.CreateProvider(),
            TableName = $"t{Guid.NewGuid():N}"
        };
        opts.MapTypeToTable<User>($"users_{Guid.NewGuid():N}");

        using var store = new DocumentStore(opts);

        await store.Insert(new User { Id = "1", Name = "Alice", Age = 30, Email = "a@test.com" });
        await store.Insert(new Product { Id = "p1", Title = "Widget", Price = 9.99m });

        Assert.Equal(1, await store.Count<User>());
        Assert.Equal(1, await store.Count<Product>());

        await store.Clear<User>();
        Assert.Equal(0, await store.Count<User>());
        Assert.Equal(1, await store.Count<Product>());
    }

    [Fact]
    public void MapTypeToTable_DuplicateTableName_Throws()
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = Fixture.CreateProvider()
        };
        opts.MapTypeToTable<User>("shared");

        Assert.Throws<ArgumentException>(() => opts.MapTypeToTable<Product>("shared"));
    }

    [Fact]
    public async Task MapTypeToTable_CrudOperationsWork()
    {
        var tableName = $"users_{Guid.NewGuid():N}";
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = Fixture.CreateProvider()
        };
        opts.MapTypeToTable<User>(tableName);

        using var store = new DocumentStore(opts);

        await store.Insert(new User { Id = "1", Name = "Alice", Age = 30, Email = "a@test.com" });

        var user = await store.Get<User>("1");
        Assert.NotNull(user);
        Assert.Equal("Alice", user.Name);

        user.Name = "Alice Updated";
        await store.Update(user);
        user = await store.Get<User>("1");
        Assert.Equal("Alice Updated", user!.Name);

        await store.Upsert(new User { Id = "1", Name = "Alice Final", Age = 31, Email = "a@test.com" });
        user = await store.Get<User>("1");
        Assert.Equal("Alice Final", user!.Name);

        Assert.Equal(1, await store.Count<User>());

        var removed = await store.Remove<User>("1");
        Assert.True(removed);
        Assert.Equal(0, await store.Count<User>());
    }

    [Fact]
    public async Task MapTypeToTable_QueryBuilderWorks()
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = Fixture.CreateProvider()
        };
        opts.MapTypeToTable<User>($"users_{Guid.NewGuid():N}");

        using var store = new DocumentStore(opts);

        await store.Insert(new User { Id = "1", Name = "Alice", Age = 30, Email = "a@test.com" });
        await store.Insert(new User { Id = "2", Name = "Bob", Age = 25, Email = "b@test.com" });

        var results = await store.Query<User>()
            .Where(u => u.Age > 20)
            .ToList();

        Assert.Equal(2, results.Count);
    }

    [Fact]
    public async Task MapTypeToTable_TransactionsWork()
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = Fixture.CreateProvider()
        };
        opts.MapTypeToTable<User>($"users_{Guid.NewGuid():N}");

        using var store = new DocumentStore(opts);

        await store.RunInTransaction(async tx =>
        {
            await tx.Insert(new User { Id = "1", Name = "Alice", Age = 30, Email = "a@test.com" });
            await tx.Insert(new User { Id = "2", Name = "Bob", Age = 25, Email = "b@test.com" });
        });

        Assert.Equal(2, await store.Count<User>());
    }

    [Fact]
    public void MapTypeToTable_FluentChaining()
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = Fixture.CreateProvider()
        };

        var result = opts
            .MapTypeToTable<User>($"users_{Guid.NewGuid():N}")
            .MapTypeToTable<Product>($"products_{Guid.NewGuid():N}");

        Assert.Same(opts, result);
    }

    // ── Custom ID property tests ────────────────────────────────────────

    [Fact]
    public async Task MapTypeToTable_CustomIdProperty_InsertAndGet()
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = Fixture.CreateProvider()
        }.MapTypeToTable<CustomIdModel>($"custom_{Guid.NewGuid():N}", x => x.UserId);

        using var store = new DocumentStore(opts);

        await store.Insert(new CustomIdModel { UserId = "u1", Name = "Alice", Age = 30 });
        var doc = await store.Get<CustomIdModel>("u1");

        Assert.NotNull(doc);
        Assert.Equal("Alice", doc.Name);
        Assert.Equal("u1", doc.UserId);
    }

    [Fact]
    public async Task MapTypeToTable_CustomIdProperty_GuidAutoGenerated()
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = Fixture.CreateProvider()
        }.MapTypeToTable<GuidCustomIdModel>($"guid_{Guid.NewGuid():N}", x => x.Key);

        using var store = new DocumentStore(opts);

        var doc = new GuidCustomIdModel { Label = "Test" };
        await store.Insert(doc);

        Assert.NotEqual(Guid.Empty, doc.Key);

        var fetched = await store.Get<GuidCustomIdModel>(doc.Key);
        Assert.NotNull(fetched);
        Assert.Equal("Test", fetched.Label);
    }

    [Fact]
    public async Task MapTypeToTable_CustomIdProperty_Update()
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = Fixture.CreateProvider()
        }.MapTypeToTable<CustomIdModel>($"custom_{Guid.NewGuid():N}", x => x.UserId);

        using var store = new DocumentStore(opts);

        await store.Insert(new CustomIdModel { UserId = "u1", Name = "Alice", Age = 30 });

        var doc = await store.Get<CustomIdModel>("u1");
        doc!.Name = "Alice Updated";
        await store.Update(doc);

        var updated = await store.Get<CustomIdModel>("u1");
        Assert.Equal("Alice Updated", updated!.Name);
    }

    [Fact]
    public async Task MapTypeToTable_CustomIdProperty_Remove()
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = Fixture.CreateProvider()
        }.MapTypeToTable<CustomIdModel>($"custom_{Guid.NewGuid():N}", x => x.UserId);

        using var store = new DocumentStore(opts);

        await store.Insert(new CustomIdModel { UserId = "u1", Name = "Alice", Age = 30 });
        Assert.Equal(1, await store.Count<CustomIdModel>());

        var removed = await store.Remove<CustomIdModel>("u1");
        Assert.True(removed);
        Assert.Equal(0, await store.Count<CustomIdModel>());
    }

    [Fact]
    public async Task MapTypeToTable_CustomIdProperty_QueryBuilder()
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = Fixture.CreateProvider()
        }.MapTypeToTable<CustomIdModel>($"custom_{Guid.NewGuid():N}", x => x.UserId);

        using var store = new DocumentStore(opts);

        await store.Insert(new CustomIdModel { UserId = "u1", Name = "Alice", Age = 30 });
        await store.Insert(new CustomIdModel { UserId = "u2", Name = "Bob", Age = 25 });

        var results = await store.Query<CustomIdModel>()
            .Where(u => u.Age > 20)
            .OrderBy(u => u.Name)
            .ToList();

        Assert.Equal(2, results.Count);
        Assert.Equal("Alice", results[0].Name);
    }

    [Fact]
    public async Task MapTypeToTable_CustomIdProperty_Transactions()
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = Fixture.CreateProvider()
        }.MapTypeToTable<CustomIdModel>($"custom_{Guid.NewGuid():N}", x => x.UserId);

        using var store = new DocumentStore(opts);

        await store.RunInTransaction(async tx =>
        {
            await tx.Insert(new CustomIdModel { UserId = "u1", Name = "Alice", Age = 30 });
            await tx.Insert(new CustomIdModel { UserId = "u2", Name = "Bob", Age = 25 });
        });

        Assert.Equal(2, await store.Count<CustomIdModel>());
    }

    [Fact]
    public async Task MapTypeToTable_CustomIdProperty_AutoDerivedTableName()
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = Fixture.CreateProvider()
        }.MapTypeToTable<CustomIdModel>(x => x.UserId);

        using var store = new DocumentStore(opts);

        await store.Insert(new CustomIdModel { UserId = "u1", Name = "Alice", Age = 30 });
        var doc = await store.Get<CustomIdModel>("u1");
        Assert.NotNull(doc);
        Assert.Equal("Alice", doc.Name);
    }

    [Fact]
    public void MapTypeToTable_CustomIdProperty_FluentChaining()
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = Fixture.CreateProvider()
        };

        var result = opts
            .MapTypeToTable<CustomIdModel>($"custom_{Guid.NewGuid():N}", x => x.UserId)
            .MapTypeToTable<GuidCustomIdModel>($"guid_{Guid.NewGuid():N}", x => x.Key);

        Assert.Same(opts, result);
    }
}
