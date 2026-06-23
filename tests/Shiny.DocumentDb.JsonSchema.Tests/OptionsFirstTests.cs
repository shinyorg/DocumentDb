using Shiny.DocumentDb.Sqlite;
using Xunit;

namespace Shiny.DocumentDb.JsonSchema.Tests;

// Verifies JSON Schema validation configured directly on DocumentStoreOptions (no DI, hand-built store).
public class OptionsFirstTests
{
    const string PersonSchema = """
    { "type": "object", "required": ["name", "email"],
      "properties": {
        "name":  { "type": "string", "minLength": 1, "maxLength": 10 },
        "email": { "type": "string", "format": "email" } } }
    """;

    const string OtherSchema = """
    { "type": "object", "required": ["whatever"],
      "properties": { "whatever": { "type": "string", "minLength": 1 } } }
    """;

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

    [Fact]
    public async Task MapJsonSchema_OnOptions_Validates_NoDI()
    {
        using var store = CreateStore(o => o.MapJsonSchema<Person>(PersonSchema));

        await store.Insert(new Person { Id = "p1", Name = "Al", Email = "a@b.com" });
        Assert.NotNull(await store.Get<Person>("p1"));

        await Assert.ThrowsAsync<DocumentSchemaValidationException>(
            () => store.Insert(new Person { Id = "p2", Name = "Al", Email = "bad" }));
    }

    [Fact]
    public async Task MapJsonSchema_AccumulatesAcrossCalls_OneInterceptor()
    {
        using var store = CreateStore(o => o
            .MapJsonSchema<Person>(PersonSchema)
            .MapJsonSchema<Other>(OtherSchema));

        await Assert.ThrowsAsync<DocumentSchemaValidationException>(
            () => store.Insert(new Person { Id = "p1", Name = "Al", Email = "bad" }));
        await Assert.ThrowsAsync<DocumentSchemaValidationException>(
            () => store.Insert(new Other { Id = "o1", Whatever = "" }));

        // both types valid -> both persist
        await store.Insert(new Person { Id = "p2", Name = "Al", Email = "a@b.com" });
        await store.Insert(new Other { Id = "o2", Whatever = "x" });
        Assert.NotNull(await store.Get<Person>("p2"));
        Assert.NotNull(await store.Get<Other>("o2"));
    }

    [Fact]
    public async Task ConfigureJsonSchemaValidation_DisablesFormatAssertion()
    {
        using var store = CreateStore(o => o
            .MapJsonSchema<Person>(PersonSchema)
            .ConfigureJsonSchemaValidation(s => s.EnableFormatAssertion = false));

        await store.Insert(new Person { Id = "p1", Name = "Al", Email = "not-an-email" });
        Assert.NotNull(await store.Get<Person>("p1"));
    }
}
