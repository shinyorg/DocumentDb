using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using Shiny.DocumentDb.Sqlite;

namespace Shiny.DocumentDb.Tests.Fixtures;

public class SqliteDatabaseFixture : IDatabaseFixture, IDocumentStoreFixture, ITenantDocumentStoreFixture, ITemporalDocumentStoreFixture
{
    public ITemporalDocumentStore CreateTemporalStore(string tableName, Action<TemporalOptions>? configure = null, Func<string>? actor = null)
    {
        var opts = new DocumentStoreOptions { DatabaseProvider = this.CreateProvider(), TableName = tableName };
        opts.ConfigureDocument<VersionedUser>(cfg => cfg.MapTemporal(o =>
        {
            configure?.Invoke(o);
            if (actor != null)
                o.CaptureActor = actor;
        }));
        opts.ConfigureDocument<MergeDoc>(cfg => cfg.MapTemporal());
        return new DocumentStore(opts);
    }

    public IDatabaseProvider CreateProvider()
        => new SqliteDatabaseProvider("Data Source=:memory:");

    public IDocumentStore CreateStore(string tableName)
        => new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = this.CreateProvider(),
            TableName = tableName
        });

    /// <summary>Builds a store with caller-supplied provider-agnostic options — the hook the
    /// cross-provider conformance suites configure themselves through.</summary>
    public IDocumentStore CreateStore(string tableName, Action<IDocumentStoreOptions> configure)
    {
        var opts = new DocumentStoreOptions { DatabaseProvider = this.CreateProvider(), TableName = tableName };
        configure(opts);
        return new DocumentStore(opts);
    }

    public IDocumentStore CreateStoreWithVersion<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(string tableName, Expression<Func<T, int>> versionProperty) where T : class
    {
        var opts = new DocumentStoreOptions { DatabaseProvider = this.CreateProvider(), TableName = tableName };
        opts.ConfigureDocument<T>(cfg => cfg.MapVersionProperty(versionProperty));
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
        opts.ConfigureDocument<VectorDoc>(cfg => cfg.MapVectorProperty(d => d.Embedding, dimensions, metric, indexKind));
        return new DocumentStore(opts);
    }

    /// <summary>
    /// A vector-enabled store whose mappings the caller supplies — for suites that bring their own record
    /// types instead of the shared <see cref="VectorDoc"/>.
    /// </summary>
    public IDocumentStore CreateVectorStore(string dbFile, Action<IDocumentStoreOptions> configure)
    {
        var binary = FindVec0BinaryPath()
            ?? throw new InvalidOperationException("vec0 native binary not found next to the test assembly.");

        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider($"Data Source={dbFile}")
            {
                EnableVectorExtension = true,
                VectorExtensionPath = binary
            }
        };
        configure(opts);
        return new DocumentStore(opts);
    }
}
