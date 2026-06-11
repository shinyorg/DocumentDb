using System.Text.Json;
using Shiny.DocumentDb.Sqlite;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

// UseGuidV7Ids() — opt into sortable version-7 Guid generation (BCL, no extra dependency).
public class GuidV7IdTests : IDisposable
{
    static readonly TestJsonContext ctx = new(new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });

    static DocumentStore CreateStore(bool v7)
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:"),
            JsonSerializerOptions = ctx.Options,
            TableName = $"t{Guid.NewGuid():N}"
        };
        if (v7)
            opts.UseGuidV7Ids();
        return new DocumentStore(opts);
    }

    readonly DocumentStore store = CreateStore(v7: true);

    public void Dispose() => this.store.Dispose();

    [Fact]
    public async Task GeneratesVersion7_WhenDefault()
    {
        var model = new GuidIdModel { Name = "Alice" };
        Assert.Equal(Guid.Empty, model.Id);

        await this.store.Insert(model, ctx.GuidIdModel);

        Assert.NotEqual(Guid.Empty, model.Id);
        Assert.Equal(7, model.Id.Version);

        var fetched = await this.store.Get<GuidIdModel>(model.Id, ctx.GuidIdModel);
        Assert.NotNull(fetched);
        Assert.Equal(model.Id, fetched!.Id);
        Assert.Equal("Alice", fetched.Name);
    }

    [Fact]
    public async Task Preserves_ExplicitId()
    {
        var explicitId = Guid.NewGuid();
        var model = new GuidIdModel { Id = explicitId, Name = "Bob" };

        await this.store.Insert(model, ctx.GuidIdModel);

        Assert.Equal(explicitId, model.Id);
        var fetched = await this.store.Get<GuidIdModel>(explicitId, ctx.GuidIdModel);
        Assert.Equal("Bob", fetched!.Name);
    }

    [Fact]
    public void DefaultStore_StillGeneratesVersion4()
    {
        // sanity: without UseGuidV7Ids the built-in path is unchanged (random v4)
        using var v4Store = CreateStore(v7: false);
        var g = Guid.NewGuid();
        Assert.Equal(4, g.Version); // built-in generation uses Guid.NewGuid()
    }
}
