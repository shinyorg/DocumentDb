using System.Text.Json;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.DependencyInjection;
using Shiny.DocumentDb.Extensions.AI.Tests.Fixtures;
using Shiny.DocumentDb.Sqlite;
using Xunit;

namespace Shiny.DocumentDb.Extensions.AI.Tests;

/// <summary>The model always sends ids as strings; the tools convert them to the document's Id type.</summary>
public class TypedIdTests : IDisposable
{
    readonly IDocumentStore store;
    readonly DocumentStoreAITools tools;
    readonly TestJsonContext ctx = new(new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });

    public TypedIdTests()
    {
        var services = new ServiceCollection();
        services.AddDocumentStore(opts =>
        {
            opts.DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:");
            opts.JsonSerializerOptions = this.ctx.Options;
        });
        services.AddDocumentStoreAITools(b =>
        {
            b.AddType(this.ctx.Ticket, capabilities: DocumentAICapabilities.All);
            b.AddType(this.ctx.Counter);
        });
        var sp = services.BuildServiceProvider();
        this.store = sp.GetRequiredService<IDocumentStore>();
        this.tools = sp.GetRequiredService<DocumentStoreAITools>();
    }

    async Task<JsonDocument> Invoke(string name, Dictionary<string, object?> args)
    {
        var fn = (AIFunction)this.tools.Tools.First(t => t is AIFunction f && f.Name == name);
        return JsonDocument.Parse(JsonSerializer.Serialize(await fn.InvokeAsync(new AIFunctionArguments(args))));
    }

    [Theory]
    [InlineData("D")]
    [InlineData("N")]
    public async Task GetById_Accepts_Guid_As_String(string format)
    {
        var id = Guid.NewGuid();
        await this.store.Insert(new Ticket { Id = id, Subject = "Broken" }, this.ctx.Ticket);

        using var doc = await this.Invoke("ticket_get_by_id", new() { ["id"] = id.ToString(format) });

        Assert.True(doc.RootElement.GetProperty("found").GetBoolean());
        Assert.Equal("Broken", doc.RootElement.GetProperty("document").GetProperty("subject").GetString());
    }

    [Fact]
    public async Task GetById_Accepts_Guid_As_JsonElement()
    {
        var id = Guid.NewGuid();
        await this.store.Insert(new Ticket { Id = id, Subject = "Broken" }, this.ctx.Ticket);
        var arg = JsonDocument.Parse($"\"{id}\"").RootElement;

        using var doc = await this.Invoke("ticket_get_by_id", new() { ["id"] = arg });

        Assert.True(doc.RootElement.GetProperty("found").GetBoolean());
    }

    [Fact]
    public async Task GetById_With_Unparseable_Guid_Is_Not_Found()
    {
        using var doc = await this.Invoke("ticket_get_by_id", new() { ["id"] = "not-a-guid" });
        Assert.False(doc.RootElement.GetProperty("found").GetBoolean());
    }

    [Fact]
    public async Task Delete_Accepts_Guid_As_String()
    {
        var id = Guid.NewGuid();
        await this.store.Insert(new Ticket { Id = id, Subject = "Broken" }, this.ctx.Ticket);

        using var doc = await this.Invoke("ticket_delete", new() { ["id"] = id.ToString() });

        Assert.True(doc.RootElement.GetProperty("deleted").GetBoolean());
        Assert.Null(await this.store.Get(id, this.ctx.Ticket));
    }

    [Fact]
    public async Task GetById_Accepts_Long_As_String()
    {
        await this.store.Insert(new Counter { Id = 42, Label = "answer" }, this.ctx.Counter);

        using var doc = await this.Invoke("counter_get_by_id", new() { ["id"] = "42" });

        Assert.True(doc.RootElement.GetProperty("found").GetBoolean());
        Assert.Equal("answer", doc.RootElement.GetProperty("document").GetProperty("label").GetString());
    }

    public void Dispose() => (this.store as IDisposable)?.Dispose();
}
