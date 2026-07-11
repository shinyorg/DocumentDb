using Shiny.DocumentDb.CockroachDb;
using Shiny.DocumentDb.Sqlite;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// DB-free assertions that DocumentFunctions.Soundex is gated by the provider's SupportsSoundex capability —
/// providers without a phonetic-matching built-in (CockroachDB, PostgreSQL without fuzzystrmatch) reject the
/// query at build time instead of emitting SOUNDEX(...) that fails on the server.
/// </summary>
public class SoundexCapabilityTests
{
    [Fact]
    public void Cockroach_Soundex_ThrowsNotSupported()
    {
        using var store = new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new CockroachDbDatabaseProvider("Host=localhost"),
            TableName = "t"
        });
        Assert.Throws<NotSupportedException>(() =>
            store.Query<User>().Where(u => DocumentFunctions.Soundex(u.Name) == "S530").ToQueryString());
    }

    [Fact]
    public void Sqlite_Soundex_IsEmitted()
    {
        // SQLite advertises SupportsSoundex (a registered UDF), so the query builds fine.
        using var store = new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:"),
            TableName = "t"
        });
        var qs = store.Query<User>().Where(u => DocumentFunctions.Soundex(u.Name) == "S530").ToQueryString();
        Assert.Contains("SOUNDEX(", qs.Sql);
    }
}
