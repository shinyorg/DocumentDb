using Shiny.DocumentDb.CockroachDb;
using Shiny.DocumentDb.MariaDb;
using Shiny.DocumentDb.MySql;
using Shiny.DocumentDb.Oracle;
using Shiny.DocumentDb.PostgreSql;
using Shiny.DocumentDb.Sqlite;
using Shiny.DocumentDb.SqlServer;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// Dialect-string tests for the <see cref="LockMode"/> hooks — no live database required. These guard the
/// thing a behavioural test can't see cheaply: that each engine gets <b>its own</b> locking syntax rather
/// than a clause that happens to parse. A provider silently returning empty here is a lock that does nothing.
/// </summary>
public class LockModeSqlTests
{
    [Fact]
    public void PostgreSql_UsesForUpdateAndForShare()
    {
        IDatabaseProvider provider = new PostgreSqlDatabaseProvider("Host=fake;");
        Assert.Equal(" FOR UPDATE", provider.BuildLockClause(LockMode.Update));
        Assert.Equal(" FOR SHARE", provider.BuildLockClause(LockMode.Share));
        Assert.Equal(string.Empty, provider.BuildLockClause(LockMode.None));
        // Postgres locks with a trailing clause, never a table hint.
        Assert.Equal(string.Empty, provider.BuildLockTableHint(LockMode.Update));
    }

    [Fact]
    public void CockroachDb_InheritsPostgreSqlLocking()
    {
        // Wire-compatible: the point of the assertion is that the subclass does NOT quietly lose locking the
        // way it loses the change feed and bulk copy.
        IDatabaseProvider provider = new CockroachDbDatabaseProvider("Host=fake;");
        Assert.Equal(" FOR UPDATE", provider.BuildLockClause(LockMode.Update));
        Assert.Equal(" FOR SHARE", provider.BuildLockClause(LockMode.Share));
    }

    [Fact]
    public void MySql_UsesForUpdateAndForShare()
    {
        IDatabaseProvider provider = new MySqlDatabaseProvider("Server=fake;");
        Assert.Equal(" FOR UPDATE", provider.BuildLockClause(LockMode.Update));
        Assert.Equal(" FOR SHARE", provider.BuildLockClause(LockMode.Share));
    }

    [Fact]
    public void MariaDb_UsesLockInShareModeForSharedLocks()
    {
        // FOR SHARE is MySQL 8 syntax MariaDB never adopted — emitting it would be a server-side syntax error.
        IDatabaseProvider provider = new MariaDbDatabaseProvider("Server=fake;");
        Assert.Equal(" FOR UPDATE", provider.BuildLockClause(LockMode.Update));
        Assert.Equal(" LOCK IN SHARE MODE", provider.BuildLockClause(LockMode.Share));
    }

    [Fact]
    public void SqlServer_UsesTableHintNotTrailingClause()
    {
        IDatabaseProvider provider = new SqlServerDatabaseProvider("Server=fake;");
        Assert.Equal(" WITH (UPDLOCK, HOLDLOCK)", provider.BuildLockTableHint(LockMode.Update));
        Assert.Equal(" WITH (HOLDLOCK)", provider.BuildLockTableHint(LockMode.Share));
        Assert.Equal(string.Empty, provider.BuildLockTableHint(LockMode.None));
        // The hint is the whole mechanism on SQL Server — there must be no trailing clause as well.
        Assert.Equal(string.Empty, provider.BuildLockClause(LockMode.Update));
    }

    [Fact]
    public void Oracle_ThrowsForSharedLockRatherThanDegrading()
    {
        IDatabaseProvider provider = new OracleDatabaseProvider("User Id=fake;");
        Assert.Equal(" FOR UPDATE", provider.BuildLockClause(LockMode.Update));
        // Oracle has no FOR SHARE. Returning empty would hand back an unlocked read to a caller who asked
        // for a lock, so this must fail loudly.
        Assert.Throws<NotSupportedException>(() => provider.BuildLockClause(LockMode.Share));
    }

    [Fact]
    public void Sqlite_EmitsNothing_TransactionBoundaryIsTheLock()
    {
        // Not an oversight: SQLite takes a whole-database write lock on BEGIN IMMEDIATE, so a plain read
        // inside the transaction already has the guarantee. Asserted so nobody "fixes" it into FOR UPDATE,
        // which SQLite does not parse.
        IDatabaseProvider provider = new SqliteDatabaseProvider(":memory:");
        Assert.Equal(string.Empty, provider.BuildLockClause(LockMode.Update));
        Assert.Equal(string.Empty, provider.BuildLockTableHint(LockMode.Update));
    }
}
