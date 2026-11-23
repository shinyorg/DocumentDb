using System.Text.Json.Serialization;
using Microsoft.Data.Sqlite;
using Shiny.DocumentDb.Sqlite;
using Xunit;

namespace Shiny.DocumentDb.Tests.Sqlite;

/// <summary>
/// SQLite full text: indexing one field of each collection element (MapFullTextCollection), the trigram tokenizer,
/// and rebuilding an existing index when its definition changes.
/// </summary>
public class SqliteFullTextDefinitionTests : IDisposable
{
    public sealed class Programme
    {
        [JsonPropertyName("t")] public string Title { get; set; } = "";
        [JsonPropertyName("d")] public string? Description { get; set; }
    }

    public sealed class Schedule
    {
        public string Id { get; set; } = "";
        public List<Programme> Programmes { get; set; } = [];
    }

    public sealed class Station
    {
        public string Id { get; set; } = "";
        public string Name { get; set; } = "";
    }

    readonly string dir = Path.Combine(Path.GetTempPath(), "docdb-ftsdef", Guid.NewGuid().ToString("N"));
    readonly string dbPath;

    public SqliteFullTextDefinitionTests()
    {
        Directory.CreateDirectory(this.dir);
        this.dbPath = Path.Combine(this.dir, "test.db");
    }

    public void Dispose()
    {
        SqliteConnection.ClearAllPools();
        Directory.Delete(this.dir, true);
    }

    DocumentStore Open(Action<DocumentStoreOptions> configure)
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider($"Data Source={this.dbPath}"),
            TableName = "docs"
        };
        configure(opts);
        return new DocumentStore(opts);
    }

    static void TitlesOnly(DocumentStoreOptions o)
        => o.ConfigureDocument<Schedule>(cfg => cfg.MapFullTextCollection(x => x.Programmes, p => p.Title));

    // the old shape: a compiled selector the database can't run, so the whole programmes JSON gets indexed
    static void WholeCollection(DocumentStoreOptions o)
        => o.ConfigureDocument<Schedule>(cfg => cfg.MapFullTextProperty(["Programmes"], x => x.Programmes.Select(p => p.Title)));

    static Schedule Day(string id, string title, string description) => new()
    {
        Id = id,
        Programmes = [new Programme { Title = title, Description = description }]
    };

    long Scalar(string sql)
    {
        using var conn = new SqliteConnection($"Data Source={this.dbPath}");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    void Execute(string sql)
    {
        using var conn = new SqliteConnection($"Data Source={this.dbPath}");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    [Fact]
    public void Bundled_sqlite_supports_the_trigram_tokenizer()
    {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT sqlite_version()";
        var version = Version.Parse((string)cmd.ExecuteScalar()!);
        Assert.True(version >= new Version(3, 34), $"SQLite {version} predates FTS5 trigram (3.34)");
    }

    [Fact]
    public async Task A_collection_mapping_indexes_only_the_element_field()
    {
        using var store = this.Open(TitlesOnly);
        await store.Insert(Day("s1", "Hockey Night", "the evening news report"));
        await store.Insert(Day("s2", "Evening News", "headlines"));

        var news = await store.FullTextSearch<Schedule>("news");
        var hockey = await store.FullTextSearch<Schedule>("hockey");

        Assert.Equal(["s2"], news.Select(x => x.Document.Id));
        Assert.Equal(["s1"], hockey.Select(x => x.Document.Id));
    }

    [Fact]
    public async Task Updates_and_bulk_imports_keep_the_element_index()
    {
        using var store = this.Open(TitlesOnly);
        await store.Insert(Day("s1", "Hockey Night", "x"));
        await store.Upsert(Day("s1", "Curling", "x"), patchIfUpdate: false);

        Assert.Empty(await store.FullTextSearch<Schedule>("hockey"));
        Assert.Single(await store.FullTextSearch<Schedule>("curling"));
    }

    [Fact]
    public async Task A_changed_definition_rebuilds_the_existing_index()
    {
        using (var store = this.Open(WholeCollection))
        {
            await store.Insert(Day("s1", "Hockey Night", "the evening news report"));
            Assert.Single(await store.FullTextSearch<Schedule>("news"));   // description indexed under the old shape
        }

        using (var store = this.Open(TitlesOnly))
        {
            Assert.Empty(await store.FullTextSearch<Schedule>("news"));
            Assert.Single(await store.FullTextSearch<Schedule>("hockey"));
        }
    }

    [Fact]
    public async Task A_database_indexed_before_the_meta_table_is_rebuilt()
    {
        using (var store = this.Open(WholeCollection))
            await store.Insert(Day("s1", "Hockey Night", "the evening news report"));

        // what a database from an earlier version looks like: no record of how it was indexed
        this.Execute("DROP TABLE docs_fts_meta");

        using (var store = this.Open(TitlesOnly))
            Assert.Empty(await store.FullTextSearch<Schedule>("news"));
    }

    [Fact]
    public async Task An_unchanged_definition_does_not_rebuild()
    {
        using (var store = this.Open(TitlesOnly))
        {
            await store.Insert(Day("s1", "Hockey Night", "x"));
            await store.Insert(Day("s2", "Curling", "x"));
        }

        // a rebuild would put this row back
        this.Execute("DELETE FROM docs_fts WHERE docId = 's2'");

        using (var store = this.Open(TitlesOnly))
        {
            Assert.Empty(await store.FullTextSearch<Schedule>("curling"));
            await store.Insert(Day("s3", "Curling Final", "x"));   // the recreated triggers still index new rows
            Assert.Equal(["s3"], (await store.FullTextSearch<Schedule>("curling")).Select(x => x.Document.Id));
        }
    }

    [Fact]
    public async Task Mapping_a_table_that_already_has_rows_indexes_them()
    {
        using (var store = this.Open(_ => { }))
            await store.Insert(Day("s1", "Hockey Night", "x"));

        using (var store = this.Open(TitlesOnly))
            Assert.Single(await store.FullTextSearch<Schedule>("hockey"));
    }

    static void TrigramNames(DocumentStoreOptions o)
        => o.ConfigureDocument<Station>(cfg => cfg.MapFullTextProperty(x => x.Name, tokenizer: FullTextTokenizer.Trigram));

    static void WordNames(DocumentStoreOptions o)
        => o.ConfigureDocument<Station>(cfg => cfg.MapFullTextProperty(x => x.Name));

    async Task SeedStationsAsync(DocumentStore store)
    {
        await store.Insert(new Station { Id = "1", Name = "Sportsnet One" });
        await store.Insert(new Station { Id = "2", Name = "BBC One" });
        await store.Insert(new Station { Id = "3", Name = "ITV One" });
        await store.Insert(new Station { Id = "4", Name = "BBC Two" });
    }

    async Task<string[]> Ids(DocumentStore store, string text)
        => (await store.FullTextSearch<Station>(text)).Select(x => x.Document.Id).OrderBy(x => x).ToArray();

    [Fact]
    public async Task Trigram_matches_substrings_case_insensitively_and_ands_terms()
    {
        using var store = this.Open(TrigramNames);
        await this.SeedStationsAsync(store);

        Assert.Equal(["1"], await this.Ids(store, "spo"));
        Assert.Equal(["1"], await this.Ids(store, "SPORTS"));
        Assert.Equal(["2"], await this.Ids(store, "bbc one"));
        Assert.Equal(["1", "2", "3"], await this.Ids(store, "one"));
    }

    [Fact]
    public async Task Trigram_ignores_terms_under_three_characters()
    {
        using var store = this.Open(TrigramNames);
        await this.SeedStationsAsync(store);

        Assert.Empty(await this.Ids(store, "sp"));
        Assert.Equal(["2", "4"], await this.Ids(store, "bbc t"));      // "t" dropped, "bbc" kept
        Assert.Empty(await this.Ids(store, "\"bb"));                    // quotes can't inject MATCH syntax
    }

    [Fact]
    public async Task Word_tokenizer_keeps_whole_word_semantics()
    {
        using var store = this.Open(WordNames);
        await this.SeedStationsAsync(store);

        Assert.Empty(await this.Ids(store, "spo"));
        Assert.Equal(["1", "2", "3"], await this.Ids(store, "one"));
    }

    [Fact]
    public async Task Switching_to_trigram_moves_the_rows_to_the_trigram_table()
    {
        using (var store = this.Open(WordNames))
        {
            await this.SeedStationsAsync(store);
            Assert.Empty(await this.Ids(store, "spo"));
        }

        using (var store = this.Open(TrigramNames))
        {
            Assert.Equal(["1"], await this.Ids(store, "spo"));
            await store.Insert(new Station { Id = "5", Name = "Sporting TV" });
            Assert.Equal(["1", "5"], await this.Ids(store, "spo"));
        }

        Assert.Equal(0, this.Scalar("SELECT COUNT(*) FROM docs_fts WHERE typeName = 'Station'"));
        Assert.Equal(5, this.Scalar("SELECT COUNT(*) FROM docs_fts_tri WHERE typeName = 'Station'"));
    }

    [Fact]
    public async Task Trigram_and_word_types_share_a_table_without_interfering()
    {
        using var store = this.Open(o =>
        {
            TrigramNames(o);
            TitlesOnly(o);
        });
        await this.SeedStationsAsync(store);
        await store.Insert(Day("s1", "Hockey Night", "x"));

        Assert.Equal(["1"], await this.Ids(store, "spo"));
        Assert.Single(await store.FullTextSearch<Schedule>("hockey"));
        Assert.Empty(await store.FullTextSearch<Schedule>("hoc"));
    }

    [Fact]
    public async Task Composable_lucene_match_uses_the_trigram_table()
    {
        using var store = this.Open(TrigramNames);
        await this.SeedStationsAsync(store);

        var hits = await store.Query<Station>()
            .Where(x => DocumentFunctions.LuceneMatch(x.Name, "spor"))
            .ToList();

        Assert.Equal(["1"], hits.Select(x => x.Id));
    }

    // element-path refusal is covered per provider in FullTextDefinitionProviderTests (SQL Server)
    [Fact]
    public async Task Providers_without_trigram_refuse_it_at_init()
    {
        var trigram = new DocumentStoreOptions { DatabaseProvider = new Fixtures.DuckDbDatabaseFixture().CreateProvider(), TableName = "docs" };
        TrigramNames(trigram);
        using var trigramStore = new DocumentStore(trigram);
        await Assert.ThrowsAsync<NotSupportedException>(() => trigramStore.Insert(new Station { Id = "1", Name = "x" }));
    }
}
