using Shiny.DocumentDb.LiteDb;
using Shiny.DocumentDb.RavenDb;
using Shiny.DocumentDb.Sqlite;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// The validate-on-build pass. <c>ConfigureDocument</c> is provider-agnostic by design, so this is where
/// "LiteDB has no vector search" gets said — once, at store construction, with every problem in one message
/// rather than one restart at a time.
/// </summary>
public class ConfigurationValidationTests
{
    sealed class Doc
    {
        public string Id { get; set; } = "";
        public string Body { get; set; } = "";
        public GeoPoint? Location { get; set; }
        public ReadOnlyMemory<float> Embedding { get; set; }
    }

    static LiteDbDocumentStoreOptions LiteDb() => new()
    {
        ConnectionString = Path.Combine(Path.GetTempPath(), $"cfgval_{Guid.NewGuid():N}.db")
    };

    [Fact]
    public void UnsupportedFeature_IsReported()
    {
        var opts = LiteDb();
        opts.ConfigureDocument<Doc>(cfg => cfg.MapVectorProperty(x => x.Embedding, dimensions: 8));

        var errors = DocumentConfigurationValidator.Collect(opts);
        var error = Assert.Single(errors);
        Assert.Contains("'Doc' maps a vector property", error);
        Assert.Contains("LiteDB does not support", error);
    }

    [Fact]
    public void EveryProblem_IsReportedTogether()
    {
        var opts = LiteDb();
        opts.ConfigureDocument<Doc>(cfg =>
        {
            cfg.MapVectorProperty(x => x.Embedding, dimensions: 8);
            cfg.MapSpatialProperty(x => x.Location);
        });

        var ex = Assert.Throws<DocumentConfigurationException>(() => DocumentConfigurationValidator.Validate(opts));
        Assert.Equal(2, ex.Errors.Count);
        Assert.Contains("2 problems", ex.Message);
        Assert.Contains(ex.Errors, e => e.Contains("vector property"));
        Assert.Contains(ex.Errors, e => e.Contains("spatial property"));
    }

    [Fact]
    public void ValidConfiguration_ReportsNothing()
    {
        var opts = LiteDb();
        opts.ConfigureDocument<Doc>(cfg =>
        {
            cfg.ToCollection("docs");
            cfg.MapFullTextProperty(x => x.Body);
        });

        Assert.Empty(DocumentConfigurationValidator.Collect(opts));
    }

    [Fact]
    public void PerTypeStorageName_OnAProviderWithoutOne_IsReported()
    {
        var opts = new RavenDbDocumentStoreOptions { Urls = ["http://localhost:8080"], Database = "test" };
        opts.ConfigureDocument<Doc>(cfg => cfg.Table = "docs");

        var error = Assert.Single(DocumentConfigurationValidator.Collect(opts));
        Assert.Contains("RavenDB has no per-type storage unit", error);
    }

    [Fact]
    public void StoreConstruction_Throws_OnAnInvalidConfiguration()
    {
        var opts = LiteDb();
        opts.ConfigureDocument<Doc>(cfg => cfg.MapVectorProperty(x => x.Embedding, dimensions: 8));

        var ex = Assert.Throws<DocumentConfigurationException>(() => new LiteDbDocumentStore(opts));
        Assert.Contains("vector property", ex.Message);
    }

    [Fact]
    public void RelationalVectorMapping_IsAllowedWithoutTheExtension()
    {
        // Base SQLite has no sqlite-vec: the mapping degrades to "no ANN index" rather than failing, and the
        // same configuration becomes searchable once Shiny.DocumentDb.Sqlite.VectorSupport is added.
        var opts = new DocumentStoreOptions { DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:") };
        opts.ConfigureDocument<Doc>(cfg => cfg.MapVectorProperty(x => x.Embedding, dimensions: 8));

        Assert.Empty(DocumentConfigurationValidator.Collect(opts));
    }

    // ── encryption conflicts ─────────────────────────────────────────────

    sealed class Secret
    {
        public string Id { get; set; } = "";
        public string Notes { get; set; } = "";
    }

    [Fact]
    public void RandomizedEncryptedProperty_InAFullTextIndex_IsReported()
    {
        var opts = new DocumentStoreOptions { DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:") };
        opts.UseEncryptor(new AesGcmDocumentEncryptor("k1", AesGcmDocumentEncryptor.GenerateKey()));
        opts.ConfigureDocument<Secret>(cfg =>
        {
            cfg.MapProperty(x => x.Notes, p => p.Encrypt());
            cfg.MapFullTextProperty(x => x.Notes);
        });

        var error = Assert.Single(DocumentConfigurationValidator.Collect(opts));
        Assert.Contains("randomized-encrypted and cannot feed a full-text index", error);
    }
}
