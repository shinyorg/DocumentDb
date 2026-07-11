using Microsoft.Data.Sqlite;
using Shiny.DocumentDb.Sqlite;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

// Regression for DEFERRED-BUGS #1: a backup export → restore round trip used to rewrite every document's
// CreatedAt/UpdatedAt to the import time, silently losing the original history. The v2 envelope now carries the
// timestamps and the Insert restore path binds them per row (v1 backups without timestamps still re-stamp now).
public class BackupTimestampFidelityTests : IDisposable
{
    readonly SqliteConnection holdOpen;
    readonly string connectionString;
    readonly string sourceTable = $"src{Guid.NewGuid():N}";
    readonly string targetTable = $"tgt{Guid.NewGuid():N}";

    public BackupTimestampFidelityTests()
    {
        this.connectionString = $"Data Source=bk_{Guid.NewGuid():N};Mode=Memory;Cache=Shared";
        this.holdOpen = new SqliteConnection(this.connectionString);
        this.holdOpen.Open();
    }

    public void Dispose() => this.holdOpen.Dispose();

    DocumentStore NewStore(string table) => new(new DocumentStoreOptions
    {
        DatabaseProvider = new SqliteDatabaseProvider(this.connectionString),
        TableName = table
    });

    [Fact]
    public async Task Export_Then_Restore_PreservesCreatedAt()
    {
        var backdated = new DateTimeOffset(2020, 1, 2, 3, 4, 5, TimeSpan.Zero);

        using var source = this.NewStore(this.sourceTable);
        await source.Insert(new User { Id = "u1", Name = "Alice", Age = 30 });

        // Backdate the stored CreatedAt/UpdatedAt so it's clearly distinct from the (near-now) import time.
        await this.SetTimestamps(this.sourceTable, "u1", backdated);

        using var ms = new MemoryStream();
        await ((IDocumentBackup)source).ExportAsync(ms);
        ms.Position = 0;

        using var target = this.NewStore(this.targetTable);
        await ((IDocumentBackup)target).RestoreAsync(ms);

        Assert.Equal("Alice", (await target.Get<User>("u1"))!.Name);

        var (created, updated) = await this.ReadTimestamps(this.targetTable, "u1");
        Assert.Equal(backdated, created);
        Assert.Equal(backdated, updated);
    }

    [Fact]
    public async Task BulkImport_V1Rows_WithoutTimestamps_StampNow()
    {
        // A RawDocument with no CreatedAt (the v1 shape) must still import, stamping the current time.
        using var store = this.NewStore(this.targetTable);
        var before = DateTimeOffset.UtcNow.AddSeconds(-5);

        await ((IDocumentBackup)store).BulkImportAsync(Rows(("u1", "User", """{"id":"u1","name":"Bob","age":20}""")));

        Assert.Equal("Bob", (await store.Get<User>("u1"))!.Name);
        var (created, _) = await this.ReadTimestamps(this.targetTable, "u1");
        Assert.True(created >= before, $"CreatedAt {created:O} should be ~now, not the epoch/default.");
    }

    static async System.Collections.Generic.IAsyncEnumerable<RawDocument> Rows(params (string id, string docType, string json)[] rows)
    {
        foreach (var (id, docType, json) in rows)
        {
            yield return new RawDocument(id, docType, System.Text.Encoding.UTF8.GetBytes(json));
            await Task.CompletedTask;
        }
    }

    async Task SetTimestamps(string table, string id, DateTimeOffset ts)
    {
        await using var cmd = this.holdOpen.CreateCommand();
        cmd.CommandText = $"UPDATE \"{table}\" SET CreatedAt = @ts, UpdatedAt = @ts WHERE Id = @id";
        cmd.Parameters.AddWithValue("@ts", ts);
        cmd.Parameters.AddWithValue("@id", id);
        await cmd.ExecuteNonQueryAsync();
    }

    async Task<(DateTimeOffset Created, DateTimeOffset Updated)> ReadTimestamps(string table, string id)
    {
        await using var cmd = this.holdOpen.CreateCommand();
        cmd.CommandText = $"SELECT CreatedAt, UpdatedAt FROM \"{table}\" WHERE Id = @id";
        cmd.Parameters.AddWithValue("@id", id);
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync(), "row not found");
        return (reader.GetFieldValue<DateTimeOffset>(0), reader.GetFieldValue<DateTimeOffset>(1));
    }
}
