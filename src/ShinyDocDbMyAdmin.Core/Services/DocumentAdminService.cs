using System.Collections.Concurrent;
using System.Data.Common;
using ShinyDocDbMyAdmin.Models;
using ShinyDocDbMyAdmin.Providers;

namespace ShinyDocDbMyAdmin.Services;

/// <summary>
/// Everything ShinyDocDbMyAdmin can do to a target database, expressed against
/// <c>IDatabaseProvider</c> so one implementation covers all nine relational backends.
/// </summary>
/// <remarks>
/// This deliberately sits below <c>IDocumentStore</c>. That API is generic over a CLR document type,
/// and an admin tool pointed at someone else's database has no types to bind to - only the shared
/// <c>Id / TypeName / Data / CreatedAt / UpdatedAt</c> envelope. What it does reuse is the provider's
/// SQL dialect (<c>JsonExtract</c>, <c>QuoteTable</c>, <c>BuildPaginationClause</c>, the index and
/// insert/update builders), so the generated SQL matches what the library itself would emit.
/// </remarks>
public sealed partial class DocumentAdminService(ConnectionManager connections, ILogger<DocumentAdminService> logger)
{
    /// <summary>The envelope columns, in the order every SELECT in this class uses.</summary>
    const string EnvelopeColumns = "Id, TypeName, Data, CreatedAt, UpdatedAt";

    readonly ConcurrentDictionary<string, IReadOnlyList<TableInfo>> tableCache = new();

    public Task<AdminConnection> Connect(string profileId, CancellationToken ct = default)
        => connections.Open(profileId, ct);

    /// <summary>Forces the next schema read to hit the database. Call after any DDL or a profile edit.</summary>
    public void InvalidateSchema(string profileId)
    {
        this.tableCache.TryRemove(profileId, out _);

        // The vector sidecar probe is derived from the table list, so it cannot outlive it.
        foreach (var key in this.vectorSidecarCache.Keys.Where(k => k.StartsWith(profileId + "|", StringComparison.Ordinal)))
            this.vectorSidecarCache.TryRemove(key, out _);
    }

    // ── Discovery ───────────────────────────────────────────────────────

    /// <summary>Opens the connection and reports whether the database answers, without changing anything.</summary>
    public async Task<string> TestConnection(string profileId, CancellationToken ct = default)
    {
        var connection = await this.Connect(profileId, ct);
        var tables = await this.ListTables(profileId, refresh: true, ct);
        var documentTables = tables.Count(t => t.IsBrowsable);

        return documentTables == 0
            ? $"Connected. No DocumentDb tables found among {tables.Count} table(s) - the store may not be initialised yet."
            : $"Connected. Found {documentTables} document table(s) of {tables.Count} total.";
    }

    public async Task<IReadOnlyList<TableInfo>> ListTables(string profileId, bool refresh = false, CancellationToken ct = default)
    {
        if (!refresh && this.tableCache.TryGetValue(profileId, out var cached))
            return cached;

        var connection = await this.Connect(profileId, ct);
        var tables = await connection.Execute(async (db, token) =>
        {
            var names = new List<string>();
            await using (var cmd = Ado.Command(db, connection.Provider.BuildListTablesSql()))
            await using (var reader = await cmd.ExecuteReaderAsync(token))
            {
                while (await reader.ReadAsync(token))
                    names.Add(Ado.Text(reader, 0));
            }

            var results = new List<TableInfo>(names.Count);
            foreach (var name in names.OrderBy(x => x, StringComparer.OrdinalIgnoreCase))
                results.Add(await Classify(db, connection, name, token));

            return (IReadOnlyList<TableInfo>)results;
        }, ct);

        this.tableCache[profileId] = tables;
        return tables;
    }

    static async Task<TableInfo> Classify(DbConnection db, AdminConnection connection, string name, CancellationToken ct)
    {
        // The sidecars are named by convention, so recognise them before paying for a probe.
        var role = name switch
        {
            _ when name.EndsWith("_history", StringComparison.OrdinalIgnoreCase) => TableRole.History,
            _ when name.EndsWith("_blobs", StringComparison.OrdinalIgnoreCase) => TableRole.Blobs,
            // Covers the sidecar itself plus SQLite's R*Tree shadow tables (_spatial_node,
            // _spatial_rowid, _spatial_parent), which are created by the engine, not the library.
            _ when name.Contains("_spatial", StringComparison.OrdinalIgnoreCase) => TableRole.Sidecar,
            _ when name.Contains("_vec", StringComparison.OrdinalIgnoreCase) => TableRole.Sidecar,
            _ when name.Contains("_fts", StringComparison.OrdinalIgnoreCase) => TableRole.Sidecar,
            _ => TableRole.Foreign
        };

        if (role != TableRole.Foreign)
            return new TableInfo(name, role);

        // Otherwise ask the database: a table carrying the whole envelope is one we can browse.
        var quoted = connection.Provider.QuoteTable(Ado.SafeIdentifier(name));
        if (!await ColumnsExist(db, $"SELECT {EnvelopeColumns} FROM {quoted} WHERE 1 = 0", ct))
            return new TableInfo(name, TableRole.Foreign);

        var hasTenant = await ColumnsExist(db, $"SELECT TenantId FROM {quoted} WHERE 1 = 0", ct);
        return new TableInfo(name, TableRole.Documents, hasTenant);
    }

    static async Task<bool> ColumnsExist(DbConnection db, string probeSql, CancellationToken ct)
    {
        try
        {
            await using var cmd = Ado.Command(db, probeSql);
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            return true;
        }
        catch (DbException)
        {
            return false;
        }
    }

    /// <summary>
    /// Creates a documents table, using the provider's own DDL so the result is byte-for-byte what
    /// <c>DocumentStore</c> would have created. Without this, pointing the tool at a database whose store
    /// has never run leaves nothing to look at - DocumentDb creates its tables lazily on first write.
    /// </summary>
    public async Task CreateDocumentsTable(string profileId, string table, CancellationToken ct = default)
    {
        var connection = await this.Connect(profileId, ct);
        connection.AssertWritable();

        var safeTable = Ado.SafeIdentifier(table.Trim());
        var provider = connection.Provider;

        await connection.Execute(async (db, token) =>
        {
            await using (var create = Ado.Command(db, provider.BuildCreateTableSql(safeTable)))
                await create.ExecuteNonQueryAsync(token);

            // The TypeName index is not optional in practice: every query the tool issues filters on it.
            await using var index = Ado.Command(db, provider.BuildCreateTypenameIndexSql(safeTable));
            await index.ExecuteNonQueryAsync(token);
        }, ct);

        this.InvalidateSchema(profileId);
        logger.LogInformation("Created documents table {Table}", safeTable);
    }

    /// <summary>The distinct <c>TypeName</c> values in a documents table, with row counts.</summary>
    public async Task<IReadOnlyList<DocumentTypeInfo>> ListTypes(string profileId, string table, CancellationToken ct = default)
    {
        var connection = await this.Connect(profileId, ct);
        var quoted = connection.Provider.QuoteTable(Ado.SafeIdentifier(table));

        return await connection.Execute(async (db, token) =>
        {
            await using var cmd = Ado.Command(db, $"SELECT TypeName, COUNT(*) FROM {quoted} GROUP BY TypeName ORDER BY TypeName");
            await using var reader = await cmd.ExecuteReaderAsync(token);

            var results = new List<DocumentTypeInfo>();
            while (await reader.ReadAsync(token))
                results.Add(new DocumentTypeInfo(Ado.Text(reader, 0), Ado.Count(reader.GetValue(1))));

            return (IReadOnlyList<DocumentTypeInfo>)results;
        }, ct);
    }

    /// <summary>Row counts plus, where the engine makes it cheap, the size of the stored JSON.</summary>
    public async Task<TableStats> GetStats(string profileId, string table, string? typeName = null, CancellationToken ct = default)
    {
        var connection = await this.Connect(profileId, ct);
        var quoted = connection.Provider.QuoteTable(Ado.SafeIdentifier(table));
        var where = typeName is null ? "" : " WHERE TypeName = @typeName";

        return await connection.Execute(async (db, token) =>
        {
            long documents;
            long types;
            await using (var cmd = Ado.Command(db, $"SELECT COUNT(*), COUNT(DISTINCT TypeName) FROM {quoted}{where}"))
            {
                if (typeName is not null)
                    Ado.Bind(cmd, "@typeName", typeName);

                await using var reader = await cmd.ExecuteReaderAsync(token);
                if (!await reader.ReadAsync(token))
                    return new TableStats(0, 0, null, null);

                documents = Ado.Count(reader.GetValue(0));
                types = Ado.Count(reader.GetValue(1));
            }

            var lengthExpr = JsonLengthExpression(connection.Descriptor.Kind);
            long? total = null;
            long? largest = null;
            try
            {
                await using var cmd = Ado.Command(db, $"SELECT SUM({lengthExpr}), MAX({lengthExpr}) FROM {quoted}{where}");
                if (typeName is not null)
                    Ado.Bind(cmd, "@typeName", typeName);

                await using var reader = await cmd.ExecuteReaderAsync(token);
                if (await reader.ReadAsync(token))
                {
                    total = reader.IsDBNull(0) ? null : Ado.Count(reader.GetValue(0));
                    largest = reader.IsDBNull(1) ? null : Ado.Count(reader.GetValue(1));
                }
            }
            catch (DbException ex)
            {
                // Size is a nicety, not a feature. Report the counts and move on.
                logger.LogDebug(ex, "Size statistics unavailable for {Table} on {Provider}", table, connection.Descriptor.Kind);
            }

            return new TableStats(documents, types, total, largest);
        }, ct);
    }

    /// <summary>
    /// Byte/char length of the JSON body. <c>IDatabaseProvider</c> has no dialect entry for this, so it is
    /// the one place the tool switches on the provider directly - callers treat a failure as "unknown".
    /// </summary>
    static string JsonLengthExpression(ProviderKind kind) => kind switch
    {
        ProviderKind.Sqlite or ProviderKind.SqlCipher => "length(Data)",
        ProviderKind.DuckDb => "length(CAST(Data AS VARCHAR))",
        ProviderKind.PostgreSql or ProviderKind.CockroachDb => "length(Data::text)",
        ProviderKind.MySql or ProviderKind.MariaDb => "CHAR_LENGTH(Data)",
        ProviderKind.SqlServer => "DATALENGTH(Data)",
        ProviderKind.Oracle => "DBMS_LOB.GETLENGTH(Data)",
        _ => "length(Data)"
    };
}
