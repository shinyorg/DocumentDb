using System.Linq.Expressions;
using Shiny.DocumentDb.Sqlite;

namespace Shiny.DocumentDb.Tests.Fixtures;

public class SqliteDatabaseFixture : IDatabaseFixture, IDocumentStoreFixture, ITenantDocumentStoreFixture
{
    public IDatabaseProvider CreateProvider()
        => new SqliteDatabaseProvider("Data Source=:memory:");

    public IDocumentStore CreateStore(string tableName)
        => new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = this.CreateProvider(),
            TableName = tableName
        });

    public IDocumentStore CreateStoreWithFilter<T>(string tableName, Expression<Func<T, bool>> filter) where T : class
    {
        var opts = new DocumentStoreOptions { DatabaseProvider = this.CreateProvider(), TableName = tableName };
        opts.AddQueryFilter(filter);
        return new DocumentStore(opts);
    }

    public IDocumentStore CreateStoreWithNamedFilter<T>(string tableName, string filterName, Expression<Func<T, bool>> filter) where T : class
    {
        var opts = new DocumentStoreOptions { DatabaseProvider = this.CreateProvider(), TableName = tableName };
        opts.AddQueryFilter(filterName, filter);
        return new DocumentStore(opts);
    }

    public IDocumentStore CreateStoreWithVersion<T>(string tableName, Expression<Func<T, int>> versionProperty) where T : class
    {
        var opts = new DocumentStoreOptions { DatabaseProvider = this.CreateProvider(), TableName = tableName };
        opts.MapVersionProperty(versionProperty);
        return new DocumentStore(opts);
    }

    public IDocumentStore CreateStoreWithTenant(string tableName, Func<string> tenantIdAccessor)
        => new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = this.CreateProvider(),
            TableName = tableName,
            TenantIdAccessor = tenantIdAccessor
        });

    /// <summary>
    /// Locates the bundled sqlite-vec binary next to the test DLL. Returns null when the
    /// binary is missing — vector tests use that to <c>Skip</c> cleanly on CI that hasn't
    /// provisioned it.
    /// </summary>
    public static string? FindVec0BinaryPath()
    {
        var dir = AppContext.BaseDirectory;
        foreach (var name in new[] { "vec0.dylib", "vec0.so", "vec0.dll" })
        {
            var p = Path.Combine(dir, name);
            if (File.Exists(p)) return p;
        }
        return null;
    }

    public IDocumentStore CreateVectorStore(string dbFile, int dimensions = 4, VectorDistance metric = VectorDistance.Cosine, VectorIndexKind indexKind = VectorIndexKind.None)
    {
        var binary = FindVec0BinaryPath()
            ?? throw new InvalidOperationException("vec0 native binary not found next to the test assembly.");

        // Use a file-backed DB so vec0 sees a stable file path. The fixture's :memory: store is
        // for the non-vector tests; vector tests own their own ephemeral file.
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider($"Data Source={dbFile}")
            {
                EnableVectorExtension = true,
                VectorExtensionPath = binary
            }
        };
        opts.MapVectorProperty<VectorDoc>(d => d.Embedding, dimensions, metric, indexKind);
        return new DocumentStore(opts);
    }
}
