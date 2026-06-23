using System.Text.Json;
using Shiny.DocumentDb.Sqlite;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

public class WriteContextJsonTests
{
    static DocumentStore CreateStore(Action<DocumentStoreOptions> configure)
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:"),
            TableName = $"t{Guid.NewGuid():N}"
        };
        configure(opts);
        return new DocumentStore(opts);
    }

    sealed class DelegateInterceptor(Action<DocumentWriteContext> before) : IDocumentInterceptor
    {
        public Task BeforeWrite(DocumentWriteContext ctx, CancellationToken ct)
        {
            before(ctx);
            return Task.CompletedTask;
        }
        public Task AfterWrite(DocumentWriteContext ctx, CancellationToken ct) => Task.CompletedTask;
    }

    [Fact]
    public async Task GetJson_ReturnsSerializedDocument_MatchesPersisted()
    {
        string? captured = null;
        using var store = CreateStore(o => o.AddInterceptor(new DelegateInterceptor(ctx => captured = ctx.GetJson())));

        await store.Insert(new User { Id = "u1", Name = "Alice", Age = 30 });

        Assert.NotNull(captured);
        using var doc = JsonDocument.Parse(captured!);
        Assert.Equal("Alice", doc.RootElement.GetProperty("name").GetString());
        Assert.Equal(30, doc.RootElement.GetProperty("age").GetInt32());

        var stored = await store.Get<User>("u1");
        Assert.Equal("Alice", stored!.Name);
    }

    [Fact]
    public async Task GetJson_ReflectsMutationByEarlierInterceptor()
    {
        string? captured = null;
        using var store = CreateStore(o => o.AddInterceptor(new DelegateInterceptor(ctx =>
        {
            if (ctx.Document is User u)
                u.Name = "Mutated";
            captured = ctx.GetJson();
        })));

        await store.Insert(new User { Id = "u1", Name = "Alice", Age = 30 });

        using var doc = JsonDocument.Parse(captured!);
        Assert.Equal("Mutated", doc.RootElement.GetProperty("name").GetString());
    }

    [Fact]
    public async Task GetJson_CacheInvalidatedWhenDocumentReplaced()
    {
        var first = new DelegateInterceptor(ctx =>
        {
            _ = ctx.GetJson();                                          // populate cache
            ctx.Document = new User { Id = "u1", Name = "Replaced", Age = 99 }; // replace -> invalidate
        });
        string? secondJson = null;
        var second = new DelegateInterceptor(ctx => secondJson = ctx.GetJson());

        using var store = CreateStore(o =>
        {
            o.AddInterceptor(first);
            o.AddInterceptor(second);
        });

        await store.Insert(new User { Id = "u1", Name = "Alice", Age = 30 });

        using var doc = JsonDocument.Parse(secondJson!);
        Assert.Equal("Replaced", doc.RootElement.GetProperty("name").GetString());
    }

    [Fact]
    public async Task GetJson_NullForDelete()
    {
        string? captured = "sentinel";
        using var store = CreateStore(o => o.AddInterceptor(new DelegateInterceptor(ctx =>
        {
            if (ctx.Operation == DocumentOperation.Delete)
                captured = ctx.GetJson();
        })));

        await store.Insert(new User { Id = "u1", Name = "Alice", Age = 30 });
        await store.Remove<User>("u1");

        Assert.Null(captured);
    }

    [Fact]
    public async Task GetJsonDocument_ParsesAndIsDisposable()
    {
        string? name = null;
        using var store = CreateStore(o => o.AddInterceptor(new DelegateInterceptor(ctx =>
        {
            using var d = ctx.GetJsonDocument();
            name = d?.RootElement.GetProperty("name").GetString();
        })));

        await store.Insert(new User { Id = "u1", Name = "Bob", Age = 22 });
        Assert.Equal("Bob", name);
    }

    [Fact]
    public async Task BatchInsert_EachContextGetJsonMatchesItsRow()
    {
        var names = new List<string>();
        using var store = CreateStore(o => o.AddInterceptor(new DelegateInterceptor(ctx =>
        {
            using var d = ctx.GetJsonDocument();
            if (d != null)
                names.Add(d.RootElement.GetProperty("name").GetString()!);
        })));

        await store.BatchInsert(new[]
        {
            new User { Id = "u1", Name = "A", Age = 1 },
            new User { Id = "u2", Name = "B", Age = 2 }
        });

        Assert.Equal(new[] { "A", "B" }, names);
    }
}
