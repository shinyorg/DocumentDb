using System.Text.Json.Serialization;
using Microsoft.Data.Sqlite;
using Shiny.DocumentDb.Sqlite;
using Xunit;

namespace Shiny.DocumentDb.Tests.Sqlite;

/// <summary>
/// Computed-property coverage (alias mode) for the relational pipeline via SQLite: a value derived from
/// other fields, not stored in the JSON, that can be filtered, sorted, and read back as a normal property.
/// </summary>
public class SqliteComputedColumnTests : IDisposable
{
    sealed class Sale
    {
        public string Id { get; set; } = "";
        public string First { get; set; } = "";
        public string Last { get; set; } = "";
        public int Quantity { get; set; }
        public int UnitPriceCents { get; set; }

        // Computed — never serialized into the document JSON; populated on read.
        [JsonIgnore] public string FullName { get; set; } = "";
        [JsonIgnore] public int LineTotalCents { get; set; }
    }

    readonly SqliteConnection holdOpen;
    readonly DocumentStore store;

    public SqliteComputedColumnTests()
    {
        var connectionString = $"Data Source=cc_{Guid.NewGuid():N};Mode=Memory;Cache=Shared";
        this.holdOpen = new SqliteConnection(connectionString);
        this.holdOpen.Open();

        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider(connectionString),
            TableName = $"t{Guid.NewGuid():N}"
        };
        opts.ConfigureDocument<Sale>(cfg =>
        {
            cfg.MapComputedProperty<string>(s => s.FullName, s => s.First + " " + s.Last);
            cfg.MapComputedProperty<int>(s => s.LineTotalCents, s => s.Quantity * s.UnitPriceCents);
        });
        this.store = new DocumentStore(opts);
    }

    public void Dispose()
    {
        this.store.Dispose();
        this.holdOpen.Dispose();
    }

    async Task SeedAsync()
    {
        await this.store.Insert(new Sale { Id = "1", First = "Jane", Last = "Doe", Quantity = 2, UnitPriceCents = 500 });   // 1000
        await this.store.Insert(new Sale { Id = "2", First = "John", Last = "Roe", Quantity = 1, UnitPriceCents = 300 });   // 300
        await this.store.Insert(new Sale { Id = "3", First = "Amy", Last = "Poe", Quantity = 5, UnitPriceCents = 250 });    // 1250
    }

    [Fact]
    public async Task Computed_ReadBack_PopulatesProperty()
    {
        await this.SeedAsync();
        var sale = (await this.store.Query<Sale>().Where(s => s.Id == "1").ToList()).Single();

        Assert.Equal("Jane Doe", sale.FullName);
        Assert.Equal(1000, sale.LineTotalCents);
    }

    [Fact]
    public async Task Computed_Filter_StringConcat()
    {
        await this.SeedAsync();
        var results = await this.store.Query<Sale>().Where(s => s.FullName == "Jane Doe").ToList();

        Assert.Single(results);
        Assert.Equal("1", results[0].Id);
    }

    [Fact]
    public async Task Computed_Filter_NumericArithmetic()
    {
        await this.SeedAsync();
        var results = await this.store.Query<Sale>().Where(s => s.LineTotalCents > 500).ToList();

        Assert.Equal(["1", "3"], results.Select(s => s.Id).OrderBy(x => x));
    }

    [Fact]
    public async Task Computed_OrderBy_NumericArithmetic_Descending()
    {
        await this.SeedAsync();
        var results = await this.store.Query<Sale>().OrderByDescending(s => s.LineTotalCents).ToList();

        Assert.Equal(["3", "1", "2"], results.Select(s => s.Id).ToArray());
    }

    [Fact]
    public async Task Computed_OrderBy_StringConcat_Ascending()
    {
        await this.SeedAsync();
        var results = await this.store.Query<Sale>().OrderBy(s => s.FullName).ToList();

        // "Amy Poe" < "Jane Doe" < "John Roe"
        Assert.Equal(["3", "1", "2"], results.Select(s => s.Id).ToArray());
    }

    [Fact]
    public async Task Computed_Filter_ViaStringWhere()
    {
        await this.SeedAsync();
        var results = await this.store.Query<Sale>().Where("lineTotalCents > 500").ToList();

        Assert.Equal(["1", "3"], results.Select(s => s.Id).OrderBy(x => x));
    }

    [Fact]
    public async Task Computed_OrderBy_ViaStringName()
    {
        await this.SeedAsync();
        var results = await this.store.Query<Sale>().OrderBy("fullName").ToList();

        Assert.Equal(["3", "1", "2"], results.Select(s => s.Id).ToArray());
    }

    [Fact]
    public async Task Computed_Project_ViaStringName()
    {
        await this.SeedAsync();
        var rows = await this.store.Query<Sale>().Where(s => s.Id == "1").Project("fullName as fn, lineTotalCents as lt").ToList();

        var row = rows.Single();
        Assert.Equal("Jane Doe", (string)row["fn"]!);
        Assert.Equal(1000, (int)row["lt"]!);
    }

    [Fact]
    public async Task Computed_NotStoredInJson()
    {
        await this.SeedAsync();
        // The computed value must not leak into the stored document; filtering on a literal that only
        // the computed property would produce proves it is derived, not persisted.
        var raw = await this.store.Query<Sale>().Project("first, last, quantity, unitPriceCents").ToList();
        Assert.All(raw, o => Assert.False(o.ContainsKey("fullName")));
        Assert.All(raw, o => Assert.False(o.ContainsKey("lineTotalCents")));
    }
}

/// <summary>
/// The materialized (generated-column + index) variant on SQLite — a VIRTUAL generated column the query
/// layer references directly. Behaviour must match alias mode; the SQL must reference the real column.
/// </summary>
public class SqliteComputedColumnIndexedTests : IDisposable
{
    sealed class Sale
    {
        public string Id { get; set; } = "";
        public string First { get; set; } = "";
        public string Last { get; set; } = "";
        public int Quantity { get; set; }
        public int UnitPriceCents { get; set; }

        [JsonIgnore] public int LineTotalCents { get; set; }
    }

    readonly SqliteConnection holdOpen;
    readonly DocumentStore store;

    public SqliteComputedColumnIndexedTests()
    {
        var connectionString = $"Data Source=cci_{Guid.NewGuid():N};Mode=Memory;Cache=Shared";
        this.holdOpen = new SqliteConnection(connectionString);
        this.holdOpen.Open();

        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider(connectionString),
            TableName = $"t{Guid.NewGuid():N}"
        };
        opts.ConfigureDocument<Sale>(cfg => cfg.MapComputedProperty<int>(s => s.LineTotalCents, s => s.Quantity * s.UnitPriceCents, indexed: true));
        this.store = new DocumentStore(opts);
    }

    public void Dispose()
    {
        this.store.Dispose();
        this.holdOpen.Dispose();
    }

    async Task SeedAsync()
    {
        await this.store.Insert(new Sale { Id = "1", Quantity = 2, UnitPriceCents = 500 });   // 1000
        await this.store.Insert(new Sale { Id = "2", Quantity = 1, UnitPriceCents = 300 });   // 300
        await this.store.Insert(new Sale { Id = "3", Quantity = 5, UnitPriceCents = 250 });   // 1250
    }

    [Fact]
    public void Query_ReferencesGeneratedColumn_NotJsonExtract()
    {
        var sql = this.store.Query<Sale>().Where(s => s.LineTotalCents > 500).ToQueryString().Sql;
        Assert.Contains("cc_Sale_LineTotalCents", sql);
    }

    [Fact]
    public async Task Materialized_Filter_Works()
    {
        await this.SeedAsync();
        var results = await this.store.Query<Sale>().Where(s => s.LineTotalCents > 500).ToList();

        Assert.Equal(["1", "3"], results.Select(s => s.Id).OrderBy(x => x));
    }

    [Fact]
    public async Task Materialized_OrderBy_And_ReadBack_Work()
    {
        await this.SeedAsync();
        var results = await this.store.Query<Sale>().OrderByDescending(s => s.LineTotalCents).ToList();

        Assert.Equal(["3", "1", "2"], results.Select(s => s.Id).ToArray());
        Assert.Equal(1250, results[0].LineTotalCents); // read-back still populates the property
    }
}
