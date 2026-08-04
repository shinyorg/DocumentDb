using System.Text.Json.Serialization;
using Shiny.DocumentDb.LiteDb;
using Xunit;

namespace Shiny.DocumentDb.Tests.LiteDb;

/// <summary>
/// Computed-property coverage for LiteDB (a client-side/in-memory provider): the value is evaluated in
/// memory and populated before filtering/ordering, so filter, sort, project, and read-back all work.
/// </summary>
public class LiteDbComputedColumnTests : IDisposable
{
    sealed class Sale
    {
        public string Id { get; set; } = "";
        public string First { get; set; } = "";
        public string Last { get; set; } = "";
        public int Quantity { get; set; }
        public int UnitPriceCents { get; set; }

        [JsonIgnore] public string FullName { get; set; } = "";
        [JsonIgnore] public int LineTotalCents { get; set; }
    }

    readonly LiteDbDocumentStore store;

    public LiteDbComputedColumnTests()
    {
        var opts = new LiteDbDocumentStoreOptions
        {
            ConnectionString = $"Filename={Path.GetTempFileName()};Connection=direct",
            CollectionName = $"t{Guid.NewGuid():N}"
        };
        opts.ConfigureDocument<Sale>(cfg =>
        {
            cfg.MapComputedProperty<string>(s => s.FullName, s => s.First + " " + s.Last);
            cfg.MapComputedProperty<int>(s => s.LineTotalCents, s => s.Quantity * s.UnitPriceCents);
        });
        this.store = new LiteDbDocumentStore(opts);
    }

    public void Dispose() => this.store.Dispose();

    async Task SeedAsync()
    {
        await this.store.Insert(new Sale { Id = "1", First = "Jane", Last = "Doe", Quantity = 2, UnitPriceCents = 500 });
        await this.store.Insert(new Sale { Id = "2", First = "John", Last = "Roe", Quantity = 1, UnitPriceCents = 300 });
        await this.store.Insert(new Sale { Id = "3", First = "Amy", Last = "Poe", Quantity = 5, UnitPriceCents = 250 });
    }

    [Fact]
    public async Task ReadBack_PopulatesProperty()
    {
        await this.SeedAsync();
        var sale = (await this.store.Query<Sale>().Where(s => s.Id == "1").ToList()).Single();
        Assert.Equal("Jane Doe", sale.FullName);
        Assert.Equal(1000, sale.LineTotalCents);
    }

    [Fact]
    public async Task Filter_ByComputed()
    {
        await this.SeedAsync();
        var results = await this.store.Query<Sale>().Where(s => s.LineTotalCents > 500).ToList();
        Assert.Equal(["1", "3"], results.Select(s => s.Id).OrderBy(x => x));
    }

    [Fact]
    public async Task OrderBy_ByComputed()
    {
        await this.SeedAsync();
        var results = await this.store.Query<Sale>().OrderByDescending(s => s.LineTotalCents).ToList();
        Assert.Equal(["3", "1", "2"], results.Select(s => s.Id).ToArray());
    }

    [Fact]
    public async Task Project_ByComputedName()
    {
        await this.SeedAsync();
        var rows = await this.store.Query<Sale>().Where(s => s.Id == "1").Project("fullName as fn, lineTotalCents as lt").ToList();
        var row = rows.Single();
        Assert.Equal("Jane Doe", (string)row["fn"]!);
        Assert.Equal(1000, (int)row["lt"]!);
    }
}
