using Shiny.DocumentDb.Sqlite;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// The v13 configuration surface: <c>ConfigureDocument&lt;T&gt;</c> and the builder it hands you. These assert
/// that each builder member lands the same mapping the flat v12 method did, that repeat blocks are additive,
/// and that the mappings a type can only have one of now refuse a second declaration instead of overwriting.
/// </summary>
public class ConfigureDocumentTests
{
    sealed class Doc
    {
        public string Id { get; set; } = "";
        public string DocKey { get; set; } = "";
        public string Title { get; set; } = "";
        public string Body { get; set; } = "";
        public int Version { get; set; }
        public bool IsDeleted { get; set; }
        public GeoPoint? Location { get; set; }
        public ReadOnlyMemory<float> Embedding { get; set; }
    }

    sealed class Other
    {
        public string Id { get; set; } = "";
    }

    static DocumentStoreOptions NewOptions() => new()
    {
        DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:")
    };

    [Fact]
    public void Table_MapsTheStorageName()
    {
        var opts = NewOptions();
        opts.ConfigureDocument<Doc>(cfg => cfg.Table = "docs");

        Assert.Equal("docs", opts.ResolveTableName(nameof(Doc)));
    }

    [Fact]
    public void Table_ReadsBackWhatWasSet()
    {
        var opts = NewOptions();
        opts.ConfigureDocument<Doc>(cfg =>
        {
            Assert.Null(cfg.Table);
            cfg.Table = "docs";
            Assert.Equal("docs", cfg.Table);
        });
    }

    [Fact]
    public void Table_TwoTypesOneName_Throws()
    {
        var opts = NewOptions();
        opts.ConfigureDocument<Doc>(cfg => cfg.Table = "shared");

        var ex = Assert.Throws<ArgumentException>(() => opts.ConfigureDocument<Other>(cfg => cfg.Table = "shared"));
        Assert.Contains("already mapped to another type", ex.Message);
    }

    [Fact]
    public void Table_SameTypeRemapped_TakesTheLastValue()
    {
        // The generated [Document] configuration runs before the user's, so a later block has to win.
        var opts = NewOptions();
        opts.ConfigureDocument<Doc>(cfg => cfg.Table = "generated");
        opts.ConfigureDocument<Doc>(cfg => cfg.Table = "handwritten");

        Assert.Equal("handwritten", opts.ResolveTableName(nameof(Doc)));

        // ...and the name it gave up is free for another type.
        opts.ConfigureDocument<Other>(cfg => cfg.Table = "generated");
        Assert.Equal("generated", opts.ResolveTableName(nameof(Other)));
    }

    [Fact]
    public void ConfigureDocument_IsAdditiveAcrossCalls()
    {
        var opts = NewOptions();
        opts.ConfigureDocument<Doc>(cfg => cfg.Table = "docs");
        opts.ConfigureDocument<Doc>(cfg => cfg.MapIdProperty(x => x.DocKey));

        Assert.Equal("docs", opts.ResolveTableName(nameof(Doc)));
        Assert.Equal(nameof(Doc.DocKey), opts.ResolveIdPropertyName(typeof(Doc)));
    }

    [Fact]
    public void MapIdProperty_ExpressionAndStringAgree()
    {
        var byExpression = NewOptions();
        byExpression.ConfigureDocument<Doc>(cfg => cfg.MapIdProperty(x => x.DocKey));

        var byName = NewOptions();
        byName.ConfigureDocument<Doc>(cfg => cfg.MapIdProperty(nameof(Doc.DocKey)));

        Assert.Equal(
            byExpression.ResolveIdPropertyName(typeof(Doc)),
            byName.ResolveIdPropertyName(typeof(Doc)));
    }

    [Fact]
    public void AddQueryFilter_RegistersNamedAndUnnamed()
    {
        var opts = NewOptions();
        opts.ConfigureDocument<Doc>(cfg =>
        {
            cfg.AddQueryFilter(x => !x.IsDeleted);
            cfg.AddQueryFilter("adults", x => x.Version > 0);
        });

        var filters = opts.ResolveQueryFilters(typeof(Doc));
        Assert.Equal(2, filters.Count);
        Assert.Contains(filters, f => f.Name == null);
        Assert.Contains(filters, f => f.Name == "adults");
    }

    [Fact]
    public void MapVersionProperty_Registers()
    {
        var opts = NewOptions();
        opts.ConfigureDocument<Doc>(cfg => cfg.MapVersionProperty(x => x.Version));

        Assert.Equal(nameof(Doc.Version), opts.ResolveVersionMapping(typeof(Doc))?.PropertyName);
    }

    [Fact]
    public void MapTemporal_Registers()
    {
        var opts = NewOptions();
        opts.ConfigureDocument<Doc>(cfg => cfg.MapTemporal(o => o.MaxVersions = 5));

        Assert.Contains(typeof(Doc), opts.Mappings.TemporalTypes);
    }

    [Fact]
    public void MapComputedProperty_TakesOneGenericArgument()
    {
        var opts = NewOptions();
        opts.ConfigureDocument<Doc>(cfg => cfg.MapComputedProperty<string>(
            nameof(Doc.Title),
            x => x.Body,
            (x, v) => x.Title = v));

        Assert.Single(opts.ResolveComputedMappings(typeof(Doc)));
    }

    [Fact]
    public async Task OnBeforeWrite_RegistersOnTheSharedPipeline()
    {
        var seen = 0;
        var opts = NewOptions();
        opts.ConfigureDocument<Doc>(cfg => cfg.OnBeforeWrite((_, _) =>
        {
            seen++;
            return Task.CompletedTask;
        }));

        using var store = new DocumentStore(opts);
        await store.Insert(new Doc { Id = "d1" });

        Assert.Equal(1, seen);
    }

    // ── one-per-type mappings now refuse a second declaration ────────────

    [Fact]
    public void MapSpatialProperty_Twice_Throws()
    {
        var opts = NewOptions();

        var ex = Assert.Throws<InvalidOperationException>(() => opts.ConfigureDocument<Doc>(cfg =>
        {
            cfg.MapSpatialProperty(x => x.Location);
            cfg.MapSpatialProperty(x => x.Location);
        }));
        Assert.Contains("already has a spatial mapping", ex.Message);
    }

    [Fact]
    public void MapVectorProperty_Twice_Throws()
    {
        var opts = NewOptions();

        var ex = Assert.Throws<InvalidOperationException>(() => opts.ConfigureDocument<Doc>(cfg =>
        {
            cfg.MapVectorProperty(x => x.Embedding, dimensions: 8);
            cfg.MapVectorProperty(x => x.Embedding, dimensions: 8);
        }));
        Assert.Contains("already has a vector mapping", ex.Message);
    }

    [Fact]
    public void MapFullTextProperty_Twice_Throws()
    {
        var opts = NewOptions();

        var ex = Assert.Throws<InvalidOperationException>(() => opts.ConfigureDocument<Doc>(cfg =>
        {
            cfg.MapFullTextProperty(x => x.Title);
            cfg.MapFullTextProperty(x => x.Body);
        }));
        Assert.Contains("already has a full-text mapping", ex.Message);
    }

    [Fact]
    public void MapFullTextProperty_SeveralFieldsInOneIndex_IsOneMapping()
    {
        var opts = NewOptions();
        opts.ConfigureDocument<Doc>(cfg => cfg.MapFullTextProperty([x => x.Title, x => x.Body]));

        var mapping = opts.ResolveFullTextMapping(typeof(Doc));
        Assert.NotNull(mapping);
        Assert.Equal([nameof(Doc.Title), nameof(Doc.Body)], mapping.PropertyNames);
    }

    // ── vector index kind defaults to the provider's own ─────────────────

    [Fact]
    public void MapVectorProperty_WithoutAnIndexKind_TakesTheProviderDefault()
    {
        var opts = NewOptions();
        opts.ConfigureDocument<Doc>(cfg => cfg.MapVectorProperty(x => x.Embedding, dimensions: 8));

        // Unresolved until the store is built; relational defaults to HNSW.
        opts.Mappings.ResolveVectorIndexKinds(VectorIndexKind.DiskAnn);
        Assert.Equal(VectorIndexKind.DiskAnn, opts.ResolveVectorMapping(typeof(Doc))!.IndexKind);
    }

    [Fact]
    public void MapVectorProperty_WithAnExplicitIndexKind_KeepsIt()
    {
        var opts = NewOptions();
        opts.ConfigureDocument<Doc>(cfg => cfg.MapVectorProperty(x => x.Embedding, dimensions: 8, indexKind: VectorIndexKind.Flat));

        opts.Mappings.ResolveVectorIndexKinds(VectorIndexKind.DiskAnn);
        Assert.Equal(VectorIndexKind.Flat, opts.ResolveVectorMapping(typeof(Doc))!.IndexKind);
    }

    // ── the model builder, the same root the generated context hook gets ──

    [Fact]
    public void ConfigureModel_ConfiguresSeveralTypes()
    {
        var opts = NewOptions();
        opts.ConfigureModel(model => model
            .Document<Doc>(cfg => cfg.Table = "docs")
            .Document<Other>(cfg => cfg.Table = "others"));

        Assert.Equal("docs", opts.ResolveTableName(nameof(Doc)));
        Assert.Equal("others", opts.ResolveTableName(nameof(Other)));
    }
}
