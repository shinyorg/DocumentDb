using System.Text;
using Shiny.DocumentDb.DuckDb;
using Xunit;

namespace Shiny.DocumentDb.Tests;

// Regression for DEFERRED-BUGS #6: an Insert-mode BulkImport into a tenant-enabled DuckDB store used to throw a
// column-count mismatch, because DuckDB's native appender is positional and appends exactly the 5 base columns
// while shared-table multi-tenancy adds a trailing TenantId column. PostgreSQL (named COPY columns) and SQL
// Server (named ColumnMappings) leave the extra column NULL; DuckDB now reports SupportsBulkCopyWithTenant=false
// so the store falls back to the multi-row INSERT (which also omits TenantId → NULL), matching those providers
// instead of failing. (Tenant-scoped bulk import remains documented-unsupported — the imported rows are
// NULL-tenant — but it must not hard-fail.)
public class DuckDbTenantBulkImportTests
{
    [Fact]
    public async Task BulkImport_Insert_IntoTenantStore_DoesNotThrow()
    {
        var tenant = "tenantA";
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = new DuckDbDatabaseProvider("Data Source=:memory:"),
            TableName = $"t{Guid.NewGuid():N}",
            TenantIdAccessor = () => tenant
        };
        using var store = new DocumentStore(opts);

        var k = Guid.NewGuid().ToString("N");
        var result = await ((IDocumentBackup)store).BulkImportAsync(Rows(
            ($"{k}-1", "User", $$"""{"id":"{{k}}-1","name":"Alice","age":25}"""),
            ($"{k}-2", "User", $$"""{"id":"{{k}}-2","name":"Bob","age":30}""")));

        Assert.Equal(2, result.DocumentsWritten);
    }

    static async IAsyncEnumerable<RawDocument> Rows(params (string id, string docType, string json)[] rows)
    {
        foreach (var (id, docType, json) in rows)
        {
            yield return new RawDocument(id, docType, Encoding.UTF8.GetBytes(json));
            await Task.CompletedTask;
        }
    }
}
