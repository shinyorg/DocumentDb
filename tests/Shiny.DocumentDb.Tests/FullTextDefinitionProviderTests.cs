using System.Text.RegularExpressions;
using Shiny.DocumentDb.DuckDb;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

public class FtdProgramme
{
    public string Title { get; set; } = "";
    public string? Description { get; set; }
}

public class FtdSchedule
{
    public string Id { get; set; } = "";
    public List<FtdProgramme> Programmes { get; set; } = [];
}

public class FtdStation
{
    public string Id { get; set; } = "";
    public string Name { get; set; } = "";
    public string Notes { get; set; } = "";
}

/// <summary>
/// The full-text definition features beyond SQLite, run against each relational provider: indexing one field of each
/// collection element (MapFullTextCollection), rebuilding an index whose definition changed, indexing rows stored
/// before the mapping, and the trigram tokenizer where the engine has one. Oracle Text and SQL Server Full-Text Search
/// are optional components, so where they are missing the search tests skip and the backing-column tests still run.
/// </summary>
public abstract class FullTextDefinitionProviderTestsBase
{
    protected abstract IDatabaseProvider CreateProvider();

    /// <summary>Skips a test that searches when the engine's full-text component isn't installed.</summary>
    protected virtual Task EnsureSearchAvailableAsync() => Task.CompletedTask;

    protected virtual bool SupportsElementPaths => true;
    protected virtual bool SupportsTrigram => false;

    /// <summary>SQL selecting the type's backing full-text column for one document; null for a provider without one (DuckDB).</summary>
    protected abstract string? IndexedTextSql(string tableName, string typeName, string id);

    protected static string NewTable() => "ftd" + Guid.NewGuid().ToString("N")[..12];

    protected DocumentStore Open(string tableName, Action<DocumentStoreOptions> configure, List<string>? log = null)
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = this.CreateProvider(),
            TableName = tableName,
            Logging = log == null ? null : sql => { lock (log) log.Add(sql); }
        };
        configure(opts);
        return new DocumentStore(opts);
    }

    static void TitlesOnly(DocumentStoreOptions o)
        => o.ConfigureDocument<FtdSchedule>(cfg => cfg.MapFullTextCollection(x => x.Programmes, p => p.Title));

    static void NotesIndexed(DocumentStoreOptions o)
        => o.ConfigureDocument<FtdStation>(cfg => cfg.MapFullTextProperty(x => x.Notes));

    static void NameIndexed(DocumentStoreOptions o)
        => o.ConfigureDocument<FtdStation>(cfg => cfg.MapFullTextProperty(x => x.Name));

    static void TrigramNames(DocumentStoreOptions o)
        => o.ConfigureDocument<FtdStation>(cfg => cfg.MapFullTextProperty(x => x.Name, tokenizer: FullTextTokenizer.Trigram));

    static FtdSchedule Day(string id, string title, string description) => new()
    {
        Id = id,
        Programmes = [new FtdProgramme { Title = title, Description = description }]
    };

    static FtdStation Station(string id, string name, string notes = "") => new() { Id = id, Name = name, Notes = notes };

    async Task<string?> IndexedTextAsync(string tableName, string typeName, string id)
    {
        var sql = this.IndexedTextSql(tableName, typeName, id)!;
        await using var conn = this.CreateProvider().CreateConnection();
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var value = await cmd.ExecuteScalarAsync();
        return value is null or DBNull ? null : Convert.ToString(value)!.ToLowerInvariant();
    }

    static async Task<string[]> Ids<T>(IDocumentStore store, string text) where T : class
        => (await store.FullTextSearch<T>(text)).Select(x => IdOf(x.Document)).OrderBy(x => x).ToArray();

    static string IdOf(object doc) => doc switch
    {
        FtdStation s => s.Id,
        FtdSchedule s => s.Id,
        _ => throw new InvalidOperationException()
    };

    // ── collection element fields ───────────────────────────────────────

    [Fact]
    public async Task A_collection_mapping_indexes_only_the_element_field()
    {
        if (!this.SupportsElementPaths)
            Assert.Skip("This provider has no element-path full-text index.");
        await this.EnsureSearchAvailableAsync();

        using var store = this.Open(NewTable(), TitlesOnly);
        await store.Insert(Day("s1", "Hockey Night", "the evening news report"));
        await store.Insert(Day("s2", "Evening News", "headlines"));

        Assert.Equal(["s2"], await Ids<FtdSchedule>(store, "news"));
        Assert.Equal(["s1"], await Ids<FtdSchedule>(store, "hockey"));
    }

    [Fact]
    public async Task An_updated_collection_reindexes_its_element_field()
    {
        if (!this.SupportsElementPaths)
            Assert.Skip("This provider has no element-path full-text index.");
        await this.EnsureSearchAvailableAsync();

        using var store = this.Open(NewTable(), TitlesOnly);
        await store.Insert(Day("s1", "Hockey Night", "x"));
        await store.Upsert(Day("s1", "Curling", "x"), patchIfUpdate: false);

        Assert.Empty(await Ids<FtdSchedule>(store, "hockey"));
        Assert.Equal(["s1"], await Ids<FtdSchedule>(store, "curling"));
    }

    [Fact]
    public async Task The_backing_column_holds_only_the_element_field()
    {
        if (!this.SupportsElementPaths || this.IndexedTextSql("t", "t", "t") == null)
            Assert.Skip("No element-path backing column on this provider.");

        var table = NewTable();
        using var store = this.Open(table, TitlesOnly);
        await store.Insert(Day("s1", "Hockey Night", "the evening news report"));

        var text = await this.IndexedTextAsync(table, nameof(FtdSchedule), "s1");
        Assert.Contains("hockey", text);
        Assert.DoesNotContain("news", text);
    }

    [Fact]
    public async Task Element_paths_are_refused_where_unsupported()
    {
        if (this.SupportsElementPaths)
            Assert.Skip("This provider supports element paths.");

        using var store = this.Open(NewTable(), TitlesOnly);
        await Assert.ThrowsAsync<NotSupportedException>(() => store.Insert(Day("s1", "x", "y")));
    }

    // ── definition changes ──────────────────────────────────────────────

    [Fact]
    public async Task A_changed_definition_rebuilds_the_index()
    {
        await this.EnsureSearchAvailableAsync();
        var table = NewTable();

        using (var store = this.Open(table, NotesIndexed))
        {
            await store.Insert(Station("1", "Sportsnet", "hockey all night"));
            Assert.Equal(["1"], await Ids<FtdStation>(store, "hockey"));
        }

        using (var store = this.Open(table, NameIndexed))
        {
            Assert.Empty(await Ids<FtdStation>(store, "hockey"));
            Assert.Equal(["1"], await Ids<FtdStation>(store, "sportsnet"));
        }
    }

    [Fact]
    public async Task A_changed_definition_rewrites_the_backing_column()
    {
        if (this.IndexedTextSql("t", "t", "t") == null)
            Assert.Skip("This provider rebuilds its index before every query.");

        var table = NewTable();
        using (var store = this.Open(table, NotesIndexed))
            await store.Insert(Station("1", "Sportsnet", "hockey all night"));
        Assert.Contains("hockey", await this.IndexedTextAsync(table, nameof(FtdStation), "1"));

        using (var store = this.Open(table, NameIndexed))
            await store.Insert(Station("2", "Jazz FM"));   // opens the table, so init runs

        var rebuilt = await this.IndexedTextAsync(table, nameof(FtdStation), "1");
        Assert.Contains("sportsnet", rebuilt);
        Assert.DoesNotContain("hockey", rebuilt);
        Assert.Contains("jazz", await this.IndexedTextAsync(table, nameof(FtdStation), "2"));
    }

    [Fact]
    public async Task An_unchanged_definition_does_not_rebuild()
    {
        if (this.IndexedTextSql("t", "t", "t") == null)
            Assert.Skip("This provider rebuilds its index before every query.");

        var table = NewTable();
        using (var store = this.Open(table, NameIndexed))
            await store.Insert(Station("1", "Sportsnet"));

        var log = new List<string>();
        using (var store = this.Open(table, NameIndexed, log))
            await store.Insert(Station("2", "Jazz FM"));

        Assert.DoesNotContain(log, sql => Regex.IsMatch(sql, @"\bDROP\s+(COLUMN|INDEX|FULLTEXT)\b", RegexOptions.IgnoreCase));
        Assert.Contains("sportsnet", await this.IndexedTextAsync(table, nameof(FtdStation), "1"));
    }

    [Fact]
    public async Task Mapping_a_table_that_already_has_rows_indexes_them()
    {
        var table = NewTable();
        using (var store = this.Open(table, _ => { }))
            await store.Insert(Station("1", "Sportsnet"));

        using (var store = this.Open(table, NameIndexed))
        {
            await store.Insert(Station("2", "Jazz FM"));
            if (this.IndexedTextSql("t", "t", "t") != null)
                Assert.Contains("sportsnet", await this.IndexedTextAsync(table, nameof(FtdStation), "1"));
        }

        await this.EnsureSearchAvailableAsync();
        using (var store = this.Open(table, NameIndexed))
            Assert.Equal(["1"], await Ids<FtdStation>(store, "sportsnet"));
    }

    // ── trigram ─────────────────────────────────────────────────────────

    async Task<IDocumentStore> TrigramStoreAsync()
    {
        if (!this.SupportsTrigram)
            Assert.Skip("This provider has no trigram full-text index.");
        await this.EnsureSearchAvailableAsync();

        var store = this.Open(NewTable(), TrigramNames);
        await store.Insert(Station("1", "Sportsnet One"));
        await store.Insert(Station("2", "BBC One"));
        await store.Insert(Station("3", "ITV One"));
        await store.Insert(Station("4", "BBC Two"));
        return store;
    }

    [Fact]
    public async Task Trigram_matches_substrings_case_insensitively_and_ands_terms()
    {
        using var store = (DocumentStore)await this.TrigramStoreAsync();

        Assert.Equal(["1"], await Ids<FtdStation>(store, "spo"));
        Assert.Equal(["1"], await Ids<FtdStation>(store, "SPORTS"));
        Assert.Equal(["2"], await Ids<FtdStation>(store, "bbc one"));
        Assert.Equal(["1", "2", "3"], await Ids<FtdStation>(store, "one"));
    }

    [Fact]
    public async Task Trigram_ignores_terms_under_three_characters()
    {
        using var store = (DocumentStore)await this.TrigramStoreAsync();

        Assert.Empty(await Ids<FtdStation>(store, "sp"));
        Assert.Equal(["2", "4"], await Ids<FtdStation>(store, "bbc t"));
        Assert.Empty(await Ids<FtdStation>(store, "%%%"));      // LIKE wildcards are literals
        Assert.Empty(await Ids<FtdStation>(store, "b_c"));
    }

    [Fact]
    public async Task Trigram_composable_lucene_match_and_score()
    {
        using var store = (DocumentStore)await this.TrigramStoreAsync();

        var match = await store.Query<FtdStation>()
            .Where(x => DocumentFunctions.LuceneMatch(x.Name, "spor OR (bbc AND NOT two)"))
            .ToList();
        Assert.Equal(["1", "2"], match.Select(x => x.Id).OrderBy(x => x));

        var ranked = await store.Query<FtdStation>()
            .Where(x => DocumentFunctions.LuceneMatch(x.Name, "bbc"))
            .OrderByDescending(x => DocumentFunctions.LuceneScore(x.Name, "bbc"))
            .ToList();
        Assert.Equal(["2", "4"], ranked.Select(x => x.Id).OrderBy(x => x));
    }

    [Fact]
    public async Task Switching_a_word_index_to_trigram_rebuilds_it()
    {
        if (!this.SupportsTrigram)
            Assert.Skip("This provider has no trigram full-text index.");
        await this.EnsureSearchAvailableAsync();

        var table = NewTable();
        using (var store = this.Open(table, NameIndexed))
        {
            await store.Insert(Station("1", "Sportsnet One"));
            Assert.Empty(await Ids<FtdStation>(store, "spo"));
        }

        using (var store = this.Open(table, TrigramNames))
            Assert.Equal(["1"], await Ids<FtdStation>(store, "spo"));

        using (var store = this.Open(table, NameIndexed))
            Assert.Equal(["1"], await Ids<FtdStation>(store, "sportsnet"));
    }

    [Fact]
    public async Task Trigram_is_refused_where_unsupported()
    {
        if (this.SupportsTrigram)
            Assert.Skip("This provider supports trigram.");

        using var store = this.Open(NewTable(), TrigramNames);
        await Assert.ThrowsAsync<NotSupportedException>(() => store.Insert(Station("1", "x")));
    }
}

[Collection("PostgreSQL")]
public class PostgreSqlFullTextDefinitionTests(PostgreSqlDatabaseFixture fx) : FullTextDefinitionProviderTestsBase
{
    protected override IDatabaseProvider CreateProvider() => fx.CreateProvider();
    protected override bool SupportsTrigram => true;
    protected override string? IndexedTextSql(string tableName, string typeName, string id)
        => $"SELECT fts_{typeName.ToLowerInvariant()}::text FROM \"{tableName}\" WHERE Id = '{id}' AND TypeName = '{typeName}'";
}

[Collection("CockroachDB")]
public class CockroachDbFullTextDefinitionTests(CockroachDbDatabaseFixture fx) : FullTextDefinitionProviderTestsBase
{
    protected override IDatabaseProvider CreateProvider() => fx.CreateProvider();
    protected override bool SupportsTrigram => true;
    protected override bool SupportsElementPaths => false;
    protected override string? IndexedTextSql(string tableName, string typeName, string id)
        => $"SELECT fts_{typeName.ToLowerInvariant()}::text FROM \"{tableName}\" WHERE Id = '{id}' AND TypeName = '{typeName}'";
}

[Collection("MySQL")]
public class MySqlFullTextDefinitionTests(MySqlDatabaseFixture fx) : FullTextDefinitionProviderTestsBase
{
    protected override IDatabaseProvider CreateProvider() => fx.CreateProvider();
    protected override string? IndexedTextSql(string tableName, string typeName, string id)
        => $"SELECT fts_{typeName} FROM `{tableName}` WHERE Id = '{id}' AND TypeName = '{typeName}'";
}

[Collection("MariaDB")]
public class MariaDbFullTextDefinitionTests(MariaDbDatabaseFixture fx) : FullTextDefinitionProviderTestsBase
{
    protected override IDatabaseProvider CreateProvider() => fx.CreateProvider();
    protected override string? IndexedTextSql(string tableName, string typeName, string id)
        => $"SELECT fts_{typeName} FROM `{tableName}` WHERE Id = '{id}' AND TypeName = '{typeName}'";
}

// A file database (not the fixture's :memory:), so a store opened again sees the first one's rows.
[Collection("DuckDB")]
public class DuckDbFullTextDefinitionTests : FullTextDefinitionProviderTestsBase, IDisposable
{
    readonly string dir = Path.Combine(Path.GetTempPath(), "docdb-duckftd", Guid.NewGuid().ToString("N"));

    public DuckDbFullTextDefinitionTests() => Directory.CreateDirectory(this.dir);
    public void Dispose() => Directory.Delete(this.dir, true);

    protected override IDatabaseProvider CreateProvider() => new DuckDbDatabaseProvider($"Data Source={Path.Combine(this.dir, "test.duckdb")}");
    protected override string? IndexedTextSql(string tableName, string typeName, string id) => null;
}

[Collection("Oracle")]
public class OracleFullTextDefinitionTests(OracleDatabaseFixture fx) : FullTextDefinitionProviderTestsBase
{
    protected override IDatabaseProvider CreateProvider() => fx.CreateProvider();
    protected override string? IndexedTextSql(string tableName, string typeName, string id)
        => $"SELECT fts_{typeName} FROM \"{tableName}\" WHERE Id = '{id}' AND TypeName = '{typeName}'";

    protected override async Task EnsureSearchAvailableAsync()
    {
        await using var conn = fx.CreateProvider().CreateConnection();
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM all_indextypes WHERE indextype_name = 'CONTEXT' AND owner = 'CTXSYS'";
        if (Convert.ToInt32(await cmd.ExecuteScalarAsync()) == 0)
            Assert.Skip("Oracle Text (CTXSYS.CONTEXT) is not installed in this environment.");
    }
}

[Collection("MSSQL")]
public class SqlServerFullTextDefinitionTests(MsSqlDatabaseFixture fx) : FullTextDefinitionProviderTestsBase
{
    protected override IDatabaseProvider CreateProvider() => fx.CreateProvider();
    protected override bool SupportsElementPaths => false;
    protected override string? IndexedTextSql(string tableName, string typeName, string id)
        => $"SELECT ftcc_{typeName} FROM [{tableName}] WHERE Id = '{id}' AND TypeName = '{typeName}'";

    protected override async Task EnsureSearchAvailableAsync()
    {
        await using var conn = fx.CreateProvider().CreateConnection();
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT CAST(ISNULL(SERVERPROPERTY('IsFullTextInstalled'), 0) AS INT)";
        if (Convert.ToInt32(await cmd.ExecuteScalarAsync()) == 0)
            Assert.Skip("SQL Server Full-Text Search is not installed in this environment.");
    }
}
