using Shiny.DocumentDb.Sqlite;
using Xunit;

namespace Shiny.DocumentDb.JsonSchema.Tests;

public class Person
{
    public string Id { get; set; } = "";
    public string Name { get; set; } = "";
    public string? Email { get; set; }
    public int Age { get; set; }
}

public class Other
{
    public string Id { get; set; } = "";
    public string Whatever { get; set; } = "";
}

public class JsonSchemaValidationTests
{
    // Schema is authored against the SERIALIZED (camelCase) property names.
    const string PersonSchema = """
    {
      "type": "object",
      "required": ["name", "email"],
      "properties": {
        "name":  { "type": "string", "minLength": 1, "maxLength": 10 },
        "email": { "type": "string", "format": "email" },
        "age":   { "type": "integer", "minimum": 0, "maximum": 130 }
      }
    }
    """;

    static DocumentStore CreateStore(Action<JsonSchemaOptions> schema)
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:"),
            TableName = $"t{Guid.NewGuid():N}"
        };
        opts.AddJsonSchemaValidation(schema);
        return new DocumentStore(opts);
    }

    [Fact]
    public async Task ValidDocument_Writes()
    {
        using var store = CreateStore(s => s.MapJsonSchema<Person>(PersonSchema));
        await store.Insert(new Person { Id = "p1", Name = "Alice", Email = "a@b.com", Age = 30 });
        Assert.NotNull(await store.Get<Person>("p1"));
    }

    [Fact]
    public async Task MissingRequired_Throws_AndPersistsNothing()
    {
        using var store = CreateStore(s => s.MapJsonSchema<Person>(PersonSchema));
        var ex = await Assert.ThrowsAsync<DocumentSchemaValidationException>(
            () => store.Insert(new Person { Id = "p1", Name = "Alice", Email = null, Age = 30 }));
        Assert.NotEmpty(ex.Errors);
        Assert.Null(await store.Get<Person>("p1"));
    }

    [Fact]
    public async Task TooLong_Throws()
    {
        using var store = CreateStore(s => s.MapJsonSchema<Person>(PersonSchema));
        await Assert.ThrowsAsync<DocumentSchemaValidationException>(
            () => store.Insert(new Person { Id = "p1", Name = "ThisNameIsWayTooLong", Email = "a@b.com" }));
    }

    [Fact]
    public async Task BadEmailFormat_Throws_ByDefault()
    {
        using var store = CreateStore(s => s.MapJsonSchema<Person>(PersonSchema));
        await Assert.ThrowsAsync<DocumentSchemaValidationException>(
            () => store.Insert(new Person { Id = "p1", Name = "Al", Email = "not-an-email" }));
    }

    [Fact]
    public async Task BadEmailFormat_Allowed_WhenAssertionDisabled()
    {
        using var store = CreateStore(s =>
        {
            s.EnableFormatAssertion = false;
            s.MapJsonSchema<Person>(PersonSchema);
        });
        await store.Insert(new Person { Id = "p1", Name = "Al", Email = "not-an-email" });
        Assert.NotNull(await store.Get<Person>("p1"));
    }

    [Fact]
    public async Task UnmappedType_PassesThrough()
    {
        using var store = CreateStore(s => s.MapJsonSchema<Person>(PersonSchema));
        await store.Insert(new Other { Id = "o1", Whatever = "x" });
        Assert.NotNull(await store.Get<Other>("o1"));
    }

    [Fact]
    public async Task Upsert_Enforces()
    {
        using var store = CreateStore(s => s.MapJsonSchema<Person>(PersonSchema));
        await Assert.ThrowsAsync<DocumentSchemaValidationException>(
            () => store.Upsert(new Person { Id = "p1", Name = "Al", Email = "bad" }));
    }

    [Fact]
    public async Task Delete_NotBlocked()
    {
        using var store = CreateStore(s => s.MapJsonSchema<Person>(PersonSchema));
        await store.Insert(new Person { Id = "p1", Name = "Al", Email = "a@b.com" });
        await store.Remove<Person>("p1");
        Assert.Null(await store.Get<Person>("p1"));
    }

    [Fact]
    public async Task Resolver_UsedWhenNoStaticMap()
    {
        var schema = global::Json.Schema.JsonSchema.FromText(PersonSchema);
        using var store = CreateStore(s => s.Resolver = t => t == typeof(Person) ? schema : null);
        await Assert.ThrowsAsync<DocumentSchemaValidationException>(
            () => store.Insert(new Person { Id = "p1", Name = "Al", Email = "bad" }));
    }

    [Fact]
    public async Task MapJsonSchemaFromFile_LoadsSchema()
    {
        var path = Path.GetTempFileName();
        await File.WriteAllTextAsync(path, PersonSchema);
        try
        {
            using var store = CreateStore(s => s.MapJsonSchemaFromFile<Person>(path));
            await Assert.ThrowsAsync<DocumentSchemaValidationException>(
                () => store.Insert(new Person { Id = "p1", Name = "Al", Email = "bad" }));
        }
        finally
        {
            File.Delete(path);
        }
    }
}
