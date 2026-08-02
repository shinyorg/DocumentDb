using Shiny.DocumentDb;
using Shiny.DocumentDb.CockroachDb;
using Shiny.DocumentDb.DuckDb;
using Shiny.DocumentDb.MariaDb;
using Shiny.DocumentDb.MySql;
using Shiny.DocumentDb.Oracle;
using Shiny.DocumentDb.PostgreSql;
using Shiny.DocumentDb.Sqlite;
using Shiny.DocumentDb.Sqlite.SqlCipher;
using Shiny.DocumentDb.SqlServer;

namespace ShinyDocDbMyAdmin.Providers;

/// <summary>The single place that knows how to turn a saved profile into a live DocumentDb provider.</summary>
public static class ProviderCatalog
{
    static readonly Dictionary<ProviderKind, ProviderDescriptor> All = new[]
    {
        new ProviderDescriptor
        {
            Kind = ProviderKind.Sqlite,
            DisplayName = "SQLite",
            Badge = "sqlite",
            ConnectionStringTemplate = "Data Source=app.db",
            IsFileBased = true,
            FileExtensions = [".db", ".db3", ".sqlite", ".sqlite3"],
            ExclusiveFileLock = false,
            Factory = r => new SqliteDatabaseProvider(r.ConnectionString)
        },
        new ProviderDescriptor
        {
            Kind = ProviderKind.SqlCipher,
            DisplayName = "SQLCipher (encrypted SQLite)",
            Badge = "sqlcipher",
            ConnectionStringTemplate = "Data Source=encrypted.db",
            IsFileBased = true,
            FileExtensions = [".db", ".db3", ".sqlite", ".sqlite3"],
            RequiresPassword = true,
            ExclusiveFileLock = false,
            // SqlCipherDatabaseProvider takes a bare file path plus the key, not a connection string.
            Factory = r => new SqlCipherDatabaseProvider(
                r.FilePath ?? SqliteConnectionStrings.ExtractDataSource(r.ConnectionString),
                r.Password ?? throw new InvalidOperationException("SQLCipher connections require a password.")
            )
        },
        new ProviderDescriptor
        {
            Kind = ProviderKind.PostgreSql,
            DisplayName = "PostgreSQL",
            Badge = "postgres",
            ConnectionStringTemplate = "Host=localhost;Port=5432;Database=mydb;Username=postgres;Password=",
            Factory = r => new PostgreSqlDatabaseProvider(r.ConnectionString)
        },
        new ProviderDescriptor
        {
            Kind = ProviderKind.SqlServer,
            DisplayName = "SQL Server",
            Badge = "mssql",
            ConnectionStringTemplate = "Server=localhost,1433;Database=mydb;User Id=sa;Password=;TrustServerCertificate=true",
            Factory = r => new SqlServerDatabaseProvider(r.ConnectionString)
        },
        new ProviderDescriptor
        {
            Kind = ProviderKind.MySql,
            DisplayName = "MySQL",
            Badge = "mysql",
            ConnectionStringTemplate = "Server=localhost;Port=3306;Database=mydb;User Id=root;Password=",
            Factory = r => new MySqlDatabaseProvider(r.ConnectionString)
        },
        new ProviderDescriptor
        {
            Kind = ProviderKind.MariaDb,
            DisplayName = "MariaDB",
            Badge = "mariadb",
            ConnectionStringTemplate = "Server=localhost;Port=3306;Database=mydb;User Id=root;Password=",
            Factory = r => new MariaDbDatabaseProvider(r.ConnectionString)
        },
        new ProviderDescriptor
        {
            Kind = ProviderKind.Oracle,
            DisplayName = "Oracle (23ai+)",
            Badge = "oracle",
            ConnectionStringTemplate = "User Id=myuser;Password=;Data Source=localhost:1521/FREEPDB1",
            Factory = r => new OracleDatabaseProvider(r.ConnectionString)
        },
        new ProviderDescriptor
        {
            Kind = ProviderKind.DuckDb,
            DisplayName = "DuckDB",
            Badge = "duckdb",
            ConnectionStringTemplate = "Data Source=analytics.duckdb",
            IsFileBased = true,
            FileExtensions = [".duckdb", ".ddb", ".db"],
            // DuckDB takes an exclusive lock on the database file for the life of the connection.
            ExclusiveFileLock = true,
            Factory = r => new DuckDbDatabaseProvider(r.ConnectionString)
        },
        new ProviderDescriptor
        {
            Kind = ProviderKind.CockroachDb,
            DisplayName = "CockroachDB",
            Badge = "cockroach",
            ConnectionStringTemplate = "Host=localhost;Port=26257;Database=mydb;Username=root;SSL Mode=Disable",
            Factory = r => new CockroachDbDatabaseProvider(r.ConnectionString)
        }
    }.ToDictionary(x => x.Kind);

    public static IReadOnlyList<ProviderDescriptor> Descriptors { get; } = All.Values.ToList();

    public static ProviderDescriptor Get(ProviderKind kind) => All[kind];

    public static IDatabaseProvider Create(ProviderKind kind, string connectionString, string? password = null, string? filePath = null)
        => All[kind].Factory(new ProviderConnectionRequest(connectionString, password, filePath));
}

/// <summary>Small helpers for the file-backed engines, whose "connection string" is really a path.</summary>
public static class SqliteConnectionStrings
{
    /// <summary>Pulls the file path out of a <c>Data Source=</c> connection string, or returns the input as-is.</summary>
    public static string ExtractDataSource(string connectionString)
    {
        foreach (var part in connectionString.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var eq = part.IndexOf('=');
            if (eq <= 0)
                continue;

            var key = part[..eq].Trim();
            if (key.Equals("Data Source", StringComparison.OrdinalIgnoreCase) ||
                key.Equals("DataSource", StringComparison.OrdinalIgnoreCase) ||
                key.Equals("Filename", StringComparison.OrdinalIgnoreCase))
            {
                return part[(eq + 1)..].Trim();
            }
        }
        return connectionString.Trim();
    }

    /// <summary>Turns a bare path into a <c>Data Source=</c> connection string, leaving real ones untouched.</summary>
    public static string EnsureDataSource(string pathOrConnectionString)
        => pathOrConnectionString.Contains('=')
            ? pathOrConnectionString
            : $"Data Source={pathOrConnectionString}";
}
