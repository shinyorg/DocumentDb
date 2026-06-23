using System.Text.Json.Nodes;
using Shiny.DocumentDb.Sqlite;
using Xunit;

namespace Shiny.DocumentDb.OData.Tests;

public class ODataEngineTests : IAsyncLifetime
{
    IDocumentStore store = null!;

    public async ValueTask InitializeAsync()
    {
        this.store = new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:"),
            TableName = "customers",
            // Align the reflection-fallback insert path with the source-gen read path so the stored
            // JSON keys match what the JsonTypeInfo<Customer> resolves to.
            JsonSerializerOptions = TestJsonContext.Default.Options
        });

        await this.store.Insert(new Customer
        {
            Id = "1", Name = "Alice", Country = "CA", Age = 30,
            Created = new DateTime(2026, 1, 1), Email = "alice@test.com",
            Address = new Address { City = "Toronto", State = "ON" }
        });
        await this.store.Insert(new Customer
        {
            Id = "2", Name = "Bob", Country = "US", Age = 40,
            Created = new DateTime(2026, 3, 1), Email = "bob@test.com",
            Address = new Address { City = "Seattle", State = "WA" }
        });
        await this.store.Insert(new Customer
        {
            Id = "3", Name = "Carol", Country = "CA", Age = 25,
            Created = new DateTime(2026, 2, 1), Email = null,
            Address = new Address { City = "Vancouver", State = "BC" }
        });
        await this.store.Insert(new Customer
        {
            Id = "4", Name = "O'Brien", Country = "CA", Age = 50,
            Created = new DateTime(2026, 4, 1), Email = "ob@test.com",
            Address = new Address { City = "Montreal", State = "QC" }
        });
    }

    public ValueTask DisposeAsync()
    {
        (this.store as IDisposable)?.Dispose();
        return ValueTask.CompletedTask;
    }

    Task<ODataResult> Run(string query)
    {
        var model = ODataQueryModelParser.ParseQueryString(query);
        return ODataDocumentQuery.Execute<Customer>(this.store, model, TestJsonContext.Default.Customer);
    }

    static string Name(JsonNode n) => n["Name"]!.GetValue<string>();

    // ── $filter comparison operators ─────────────────────────────────────────

    [Fact]
    public async Task Filter_Eq()
    {
        var r = await Run("$filter=Country eq 'CA'&$orderby=Name");
        Assert.Equal(3, r.Value.Count);
        Assert.All(r.Value, n => Assert.Equal("CA", n["Country"]!.GetValue<string>()));
    }

    [Fact]
    public async Task Filter_Ne()
    {
        var r = await Run("$filter=Country ne 'CA'");
        Assert.Single(r.Value);
        Assert.Equal("Bob", Name(r.Value[0]));
    }

    [Fact]
    public async Task Filter_Gt()
    {
        var r = await Run("$filter=Age gt 30&$orderby=Age");
        Assert.Equal(2, r.Value.Count);
        Assert.Equal("Bob", Name(r.Value[0]));
        Assert.Equal("O'Brien", Name(r.Value[1]));
    }

    [Fact]
    public async Task Filter_Ge()
    {
        var r = await Run("$filter=Age ge 30");
        Assert.Equal(3, r.Value.Count);
    }

    [Fact]
    public async Task Filter_Lt()
    {
        var r = await Run("$filter=Age lt 30");
        Assert.Single(r.Value);
        Assert.Equal("Carol", Name(r.Value[0]));
    }

    [Fact]
    public async Task Filter_Le()
    {
        var r = await Run("$filter=Age le 30");
        Assert.Equal(2, r.Value.Count);
    }

    // ── logical operators ────────────────────────────────────────────────────

    [Fact]
    public async Task Filter_And()
    {
        var r = await Run("$filter=Country eq 'CA' and Age gt 30");
        Assert.Single(r.Value);
        Assert.Equal("O'Brien", Name(r.Value[0]));
    }

    [Fact]
    public async Task Filter_Or()
    {
        var r = await Run("$filter=Country eq 'US' or Age lt 30&$orderby=Name");
        Assert.Equal(2, r.Value.Count);
        Assert.Equal("Bob", Name(r.Value[0]));
        Assert.Equal("Carol", Name(r.Value[1]));
    }

    [Fact]
    public async Task Filter_Not()
    {
        var r = await Run("$filter=not (Country eq 'CA')");
        Assert.Single(r.Value);
        Assert.Equal("Bob", Name(r.Value[0]));
    }

    // ── string functions ─────────────────────────────────────────────────────

    [Fact]
    public async Task Filter_Contains()
    {
        var r = await Run("$filter=contains(Name, 'ar')");
        Assert.Single(r.Value);
        Assert.Equal("Carol", Name(r.Value[0]));
    }

    [Fact]
    public async Task Filter_StartsWith()
    {
        var r = await Run("$filter=startswith(Name, 'A')");
        Assert.Single(r.Value);
        Assert.Equal("Alice", Name(r.Value[0]));
    }

    [Fact]
    public async Task Filter_EndsWith()
    {
        var r = await Run("$filter=endswith(Name, 'b')");
        Assert.Single(r.Value);
        Assert.Equal("Bob", Name(r.Value[0]));
    }

    // ── null checks ──────────────────────────────────────────────────────────

    [Fact]
    public async Task Filter_EqNull()
    {
        var r = await Run("$filter=Email eq null");
        Assert.Single(r.Value);
        Assert.Equal("Carol", Name(r.Value[0]));
    }

    [Fact]
    public async Task Filter_NeNull()
    {
        var r = await Run("$filter=Email ne null");
        Assert.Equal(3, r.Value.Count);
    }

    // ── nested property path ─────────────────────────────────────────────────

    [Fact]
    public async Task Filter_NestedPath()
    {
        var r = await Run("$filter=Address/City eq 'Toronto'");
        Assert.Single(r.Value);
        Assert.Equal("Alice", Name(r.Value[0]));
    }

    // ── string-literal quote escaping ────────────────────────────────────────

    [Fact]
    public async Task Filter_QuoteEscaping()
    {
        var r = await Run("$filter=Name eq 'O''Brien'");
        Assert.Single(r.Value);
        Assert.Equal("O'Brien", Name(r.Value[0]));
    }

    // ── $orderby ─────────────────────────────────────────────────────────────

    [Fact]
    public async Task OrderBy_Ascending()
    {
        var r = await Run("$orderby=Age");
        var ages = r.Value.Select(n => n["Age"]!.GetValue<int>()).ToArray();
        Assert.Equal(new[] { 25, 30, 40, 50 }, ages);
    }

    [Fact]
    public async Task OrderBy_Descending()
    {
        var r = await Run("$orderby=Age desc");
        var ages = r.Value.Select(n => n["Age"]!.GetValue<int>()).ToArray();
        Assert.Equal(new[] { 50, 40, 30, 25 }, ages);
    }

    // ── $top / $skip → Paginate ──────────────────────────────────────────────

    [Fact]
    public async Task TopSkip_Paginate()
    {
        var r = await Run("$orderby=Age&$skip=1&$top=2");
        var ages = r.Value.Select(n => n["Age"]!.GetValue<int>()).ToArray();
        Assert.Equal(new[] { 30, 40 }, ages);
    }

    // ── $count ───────────────────────────────────────────────────────────────

    [Fact]
    public async Task Count_PresPagingTotal()
    {
        var r = await Run("$filter=Country eq 'CA'&$count=true&$top=1");
        Assert.Equal(3, r.Count);
        Assert.Single(r.Value); // page is limited
    }

    [Fact]
    public async Task Count_OmittedWhenNotRequested()
    {
        var r = await Run("$filter=Country eq 'CA'");
        Assert.Null(r.Count);
    }

    // ── $select → Project ────────────────────────────────────────────────────

    [Fact]
    public async Task Select_ProjectsFields()
    {
        var r = await Run("$filter=Name eq 'Alice'&$select=Name,Country");
        Assert.Single(r.Value);
        var obj = Assert.IsType<JsonObject>(r.Value[0]);
        Assert.True(obj.ContainsKey("Name"));
        Assert.True(obj.ContainsKey("Country"));
        Assert.False(obj.ContainsKey("Age"));
    }

    // ── $expand → 501 ────────────────────────────────────────────────────────

    [Fact]
    public async Task Expand_Throws501()
    {
        var model = ODataQueryModelParser.ParseQueryString("$expand=Address");
        await Assert.ThrowsAsync<ODataNotSupportedException>(
            () => ODataDocumentQuery.Execute<Customer>(this.store, model, TestJsonContext.Default.Customer));
    }
}
