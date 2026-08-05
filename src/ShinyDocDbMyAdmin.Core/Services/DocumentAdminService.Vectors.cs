using System.Collections.Concurrent;
using System.Data.Common;
using System.Text.Json;
using System.Text.Json.Nodes;
using ShinyDocDbMyAdmin.Models;
using Shiny.DocumentDb;
using Shiny.DocumentDb.Internal;

namespace ShinyDocDbMyAdmin.Services;

/// <summary>
/// Reading and repairing embeddings.
/// </summary>
/// <remarks>
/// Like geometry, vectors are read from the document body rather than from the sidecar: the body is
/// what the library serialises the mapped <c>ReadOnlyMemory&lt;float&gt;</c> property into, the
/// sidecar is an index built from it, and only the body has a shape that is the same on every
/// backend. The sidecar matters for the other half of the job - it is what an ANN query actually
/// searches, so a sidecar that disagrees with the bodies returns wrong answers silently, and this
/// tool both reports that and can put it right.
/// </remarks>
public sealed partial class DocumentAdminService
{
    /// <summary>How many documents a vector view or search will read before it stops and says so.</summary>
    public const int VectorScanLimit = 2000;

    readonly ConcurrentDictionary<string, VectorSidecarInfo> vectorSidecarCache = new();

    // ── Discovery ───────────────────────────────────────────────────────

    /// <summary>
    /// The JSON paths at which this type stores embeddings, found by sampling documents and looking
    /// for arrays of numbers. There is no registered mapping to consult - the tool has no CLR type -
    /// so the shape of the data is the only evidence there is.
    /// </summary>
    public async Task<IReadOnlyList<VectorFieldInfo>> FindVectorPaths(
        string profileId,
        string table,
        string typeName,
        CancellationToken ct = default)
    {
        var bodies = await this.SampleBodies(profileId, table, typeName, SchemaSampleSize, ct);
        var accumulators = new Dictionary<string, DimensionAccumulator>(StringComparer.Ordinal);
        var order = new List<string>();
        var parsed = 0;

        foreach (var body in bodies)
        {
            if (Parse(body) is JsonObject obj)
            {
                parsed++;
                foreach (var (path, length) in ScanForVectors(obj, prefix: ""))
                {
                    if (!accumulators.TryGetValue(path, out var accumulator))
                    {
                        accumulator = new DimensionAccumulator();
                        accumulators[path] = accumulator;
                        order.Add(path);
                    }

                    accumulator.Observe(length);
                }
            }
        }

        return
        [
            .. order.Select(path =>
            {
                var a = accumulators[path];
                return new VectorFieldInfo(path, a.Commonest, a.Ragged, a.Occurrences, parsed);
            })
        ];
    }

    /// <summary>Walks a document body for arrays of numbers, returning their dotted paths and lengths.</summary>
    static IEnumerable<(string Path, int Length)> ScanForVectors(JsonObject obj, string prefix, int depth = 0)
    {
        // Same three-level budget as geometry discovery: deep enough for Content.Embedding, shallow
        // enough not to walk every element of a large document.
        if (depth > 3)
            yield break;

        foreach (var (key, value) in obj)
        {
            var path = prefix.Length == 0 ? key : $"{prefix}.{key}";

            switch (value)
            {
                case JsonArray array when VectorMath.LooksLikeVector(array):
                    yield return (path, array.Count);
                    break;

                case JsonObject nested:
                    foreach (var found in ScanForVectors(nested, path, depth + 1))
                        yield return found;
                    break;
            }
        }
    }

    sealed class DimensionAccumulator
    {
        readonly Dictionary<int, int> lengths = [];

        public int Occurrences { get; private set; }
        public bool Ragged => this.lengths.Count > 1;
        public int Commonest => this.lengths.Count == 0 ? 0 : this.lengths.MaxBy(x => x.Value).Key;

        public void Observe(int length)
        {
            this.Occurrences++;
            this.lengths[length] = this.lengths.GetValueOrDefault(length) + 1;
        }
    }

    // ── The sidecar ─────────────────────────────────────────────────────

    /// <summary>
    /// Establishes what can be done with a type's vector sidecar: whether the backend has vectors at
    /// all, whether the sidecar has been created, whether this tool can write it, and how many rows
    /// it holds.
    /// </summary>
    /// <remarks>
    /// Writability is probed with a real (empty) read of the sidecar rather than inferred, because the
    /// answer is not a property of the provider. On SQLite the sidecar is a <c>vec0</c> virtual table
    /// and every statement against it fails unless the sqlite-vec extension is loaded on the
    /// connection - which the library does and this tool, which does not ship the native binaries,
    /// does not. Finding that out here rather than mid-transaction matters: on PostgreSQL a failed
    /// statement poisons the whole transaction, so a write path must know before it starts, not
    /// try-and-catch.
    /// </remarks>
    public async Task<VectorSidecarInfo> DescribeVectorSidecar(
        string profileId,
        string table,
        string typeName,
        bool refresh = false,
        CancellationToken ct = default)
    {
        var key = $"{profileId}|{table}|{typeName}";
        if (!refresh && this.vectorSidecarCache.TryGetValue(key, out var cached))
            return cached;

        var connection = await this.Connect(profileId, ct);
        var provider = connection.Provider;
        var safeTable = Ado.SafeIdentifier(table);
        var name = provider.VectorTableName(safeTable, typeName);

        var info = await this.Probe(connection, safeTable, typeName, name, profileId, ct);
        this.vectorSidecarCache[key] = info;
        return info;
    }

    async Task<VectorSidecarInfo> Probe(
        AdminConnection connection,
        string safeTable,
        string typeName,
        string sidecarName,
        string profileId,
        CancellationToken ct)
    {
        var provider = connection.Provider;

        // Deliberately not short-circuited on SupportsVector. That reports whether *this* provider
        // instance would emit vector SQL, which on SQLite is a configuration flag
        // (EnableVectorExtension / VectorExtensionPreloaded) rather than a fact about the database -
        // and this tool sets neither. The question here is whether the database has a sidecar that
        // document writes must carry, and only the table list answers that.
        var tables = await this.ListTables(profileId, refresh: false, ct);
        if (!tables.Any(t => t.Name.Equals(sidecarName, StringComparison.OrdinalIgnoreCase)))
            return new VectorSidecarInfo(sidecarName, provider.SupportsVector, false, null, null);

        var quoted = provider.QuoteTable(sidecarName);
        var idsSql = provider.BuildVectorDocIdsSql(safeTable, typeName);

        return await connection.Execute(async (db, token) =>
        {
            string? unavailable = null;
            try
            {
                await using var probe = Ado.Command(db, $"SELECT 1 FROM {quoted} WHERE 1 = 0");
                await using var reader = await probe.ExecuteReaderAsync(token);
            }
            catch (DbException ex)
            {
                unavailable = ex.Message;
            }

            long? rows = null;
            if (idsSql is not null)
            {
                try
                {
                    rows = (await ReadDocIds(db, transaction: null, idsSql, typeName, token)).Count;
                }
                catch (DbException ex)
                {
                    logger.LogDebug(ex, "Vector sidecar {Sidecar} could not be counted", sidecarName);
                }
            }

            return new VectorSidecarInfo(sidecarName, provider.SupportsVector, true, rows, unavailable);
        }, ct);
    }

    /// <summary>True when this type has anything for the Vectors tab to show.</summary>
    public async Task<bool> HasVectors(string profileId, string table, string typeName, CancellationToken ct = default)
    {
        var sidecar = await this.DescribeVectorSidecar(profileId, table, typeName, refresh: false, ct);
        if (sidecar.Present)
            return true;

        // No sidecar is not the same as no embeddings: a type can carry a float array the library was
        // never told to index, and seeing that is the point of the tab.
        return (await this.FindVectorPaths(profileId, table, typeName, ct)).Count > 0;
    }

    // ── Reading values ──────────────────────────────────────────────────

    /// <summary>
    /// Reads the embeddings at one path across a type, reduced to per-document statistics. Bounded by
    /// <paramref name="limit"/>, which the caller is expected to show rather than hide.
    /// </summary>
    public async Task<VectorSet> LoadVectors(
        string profileId,
        string table,
        string typeName,
        string path,
        int limit = VectorScanLimit,
        CancellationToken ct = default)
    {
        var entries = new List<VectorEntry>();
        var scannedIds = new HashSet<string>(StringComparer.Ordinal);
        var without = 0;
        var truncated = false;

        // Sorted by id so two loads of an unchanged table agree.
        var query = new BrowseQuery { Sort = new BrowseSort("Id", false, true) };

        await foreach (var row in this.StreamAll(profileId, table, typeName, query, ct))
        {
            if (scannedIds.Count >= limit)
            {
                truncated = true;
                break;
            }

            scannedIds.Add(row.Id);
            if (VectorMath.Read(row.Read(path)) is { } values)
                entries.Add(VectorEntry.Describe(row.Id, values));
            else
                without++;
        }

        return new VectorSet(path, entries, scannedIds.Count, without, truncated, scannedIds);
    }

    /// <summary>Reads one document's embedding. Null when the document or the path has none.</summary>
    public async Task<float[]?> ReadVector(
        string profileId,
        string table,
        string typeName,
        string id,
        string path,
        CancellationToken ct = default)
    {
        var row = await this.GetDocument(profileId, table, typeName, id, ct);
        return row is null ? null : VectorMath.Read(row.Read(path));
    }

    // ── Health ──────────────────────────────────────────────────────────

    /// <summary>
    /// Compares the sidecar against the document bodies. Everything this reports is a way an ANN query
    /// can return a wrong answer without erroring: a document whose embedding never reached the index,
    /// a row left behind by a delete, or a dimension that does not match its neighbours.
    /// </summary>
    /// <param name="set">
    /// The scan <see cref="LoadVectors"/> already performed. Passed in rather than repeated: the
    /// comparison needs the same pass over the same documents, and scanning a large type twice to
    /// render one tab is not a cost worth paying.
    /// </param>
    public async Task<VectorHealth> CheckVectorHealth(
        string profileId,
        string table,
        string typeName,
        VectorSet set,
        CancellationToken ct = default)
    {
        var sidecar = await this.DescribeVectorSidecar(profileId, table, typeName, refresh: true, ct);

        var withVector = set.Entries.Select(e => e.DocumentId).ToList();
        var dimensions = new SortedSet<int>(set.Entries.Select(e => e.Dimensions));

        VectorHealth Unreconciled() => new(
            sidecar, set.Path, withVector.Count, set.DocumentsScanned, set.Truncated,
            [], [], [.. dimensions]);

        if (!sidecar.Present)
            return Unreconciled();

        var sidecarIds = await this.ReadSidecarIds(profileId, table, typeName, ct);
        if (sidecarIds is null)
            return Unreconciled();

        var missing = withVector.Where(id => !sidecarIds.Contains(id)).ToList();

        // Only meaningful over a complete scan: with a truncated one, every unseen document looks like
        // an orphan. Reported as none rather than as a fabricated list.
        var orphaned = set.Truncated
            ? []
            : sidecarIds.Where(id => !set.ScannedIds.Contains(id)).OrderBy(x => x, StringComparer.Ordinal).ToList();

        return new VectorHealth(
            sidecar, set.Path, withVector.Count, set.DocumentsScanned, set.Truncated,
            missing, orphaned, [.. dimensions]);
    }

    /// <summary>The document ids currently in the sidecar, or null when they cannot be read.</summary>
    async Task<HashSet<string>?> ReadSidecarIds(string profileId, string table, string typeName, CancellationToken ct)
    {
        var connection = await this.Connect(profileId, ct);
        var sql = connection.Provider.BuildVectorDocIdsSql(Ado.SafeIdentifier(table), typeName);
        if (sql is null)
            return null;

        try
        {
            return await connection.Execute((db, token) => ReadDocIds(db, null, sql, typeName, token), ct);
        }
        catch (DbException ex)
        {
            logger.LogDebug(ex, "Vector sidecar ids unavailable for {Type} in {Table}", typeName, table);
            return null;
        }
    }

    static async Task<HashSet<string>> ReadDocIds(
        DbConnection db,
        DbTransaction? transaction,
        string sql,
        string typeName,
        CancellationToken ct)
    {
        await using var cmd = Ado.Command(db, sql);
        cmd.Transaction = transaction;

        // Per-type sidecars (sqlite-vec's map table) have no typeName column and so no parameter to
        // bind; binding one the statement never mentions is an error on some drivers.
        if (sql.Contains("@vecTypeName", StringComparison.Ordinal))
            Ado.Bind(cmd, "@vecTypeName", typeName);

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        var ids = new HashSet<string>(StringComparer.Ordinal);
        while (await reader.ReadAsync(ct))
            ids.Add(Ado.Text(reader, 0));

        return ids;
    }

    // ── Similarity search ───────────────────────────────────────────────

    /// <summary>
    /// Ranks documents by distance from a probe vector, computed here rather than pushed down - see
    /// <see cref="VectorMath"/> for why. Results are exact over the documents scanned.
    /// </summary>
    public async Task<IReadOnlyList<VectorNeighbour>> FindNearest(
        string profileId,
        string table,
        string typeName,
        string path,
        float[] probe,
        int k,
        VectorDistance metric = VectorDistance.Cosine,
        int limit = VectorScanLimit,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(probe);
        if (probe.Length == 0)
            throw new ArgumentException("The probe vector is empty.", nameof(probe));

        var scored = new List<VectorNeighbour>();
        var scanned = 0;
        var mismatched = 0;

        var query = new BrowseQuery { Sort = new BrowseSort("Id", false, true) };
        await foreach (var row in this.StreamAll(profileId, table, typeName, query, ct))
        {
            if (scanned >= limit)
                break;

            scanned++;
            if (VectorMath.Read(row.Read(path)) is { } values)
            {
                if (values.Length == probe.Length)
                {
                    var score = VectorMath.Distance(values, probe, metric);
                    scored.Add(new VectorNeighbour(
                        row.Id, score, VectorMath.Norm(values), values.Length, Summarize(row, path)));
                }
                else
                {
                    // A different dimension is not "far away", it is not comparable at all. Counted so
                    // the caller can say so, never silently ranked.
                    mismatched++;
                }
            }
        }

        if (mismatched > 0)
            logger.LogInformation("Skipped {Count} {Type} document(s) whose vector dimension differs from the probe", mismatched, typeName);

        return [.. scored.OrderBy(x => x.Score).Take(Math.Max(1, k))];
    }

    /// <summary>
    /// A short human label for a search hit: the first scalar string in the body that is not the
    /// embedding, so results are recognisable without opening each one.
    /// </summary>
    static string Summarize(DocumentRow row, string vectorPath)
    {
        if (row.Body is not JsonObject obj)
            return "";

        foreach (var (key, value) in obj)
        {
            var isText = value is JsonValue v
                         && v.GetValueKind() == JsonValueKind.String
                         && !key.Equals(vectorPath, StringComparison.OrdinalIgnoreCase)
                         && !key.Equals("id", StringComparison.OrdinalIgnoreCase);

            if (isText)
            {
                var text = value!.GetValue<string>();
                return text.Length > 80 ? text[..77] + "..." : text;
            }
        }

        return "";
    }

    // ── Writes ──────────────────────────────────────────────────────────

    /// <summary>
    /// Rebuilds the sidecar from the document bodies, which are the source of truth. This is the
    /// repair for everything <see cref="CheckVectorHealth"/> reports.
    /// </summary>
    /// <returns>How many sidecar rows were written.</returns>
    public async Task<int> ResyncVectorSidecar(
        string profileId,
        string table,
        string typeName,
        string path,
        int limit = VectorScanLimit,
        CancellationToken ct = default)
    {
        var connection = await this.Connect(profileId, ct);
        connection.AssertWritable();

        var sidecar = await this.DescribeVectorSidecar(profileId, table, typeName, refresh: true, ct);
        if (!sidecar.Present)
            throw new InvalidOperationException($"There is no vector sidecar '{sidecar.TableName}' to rebuild.");

        if (sidecar.Unavailable is not null)
            throw new InvalidOperationException(
                $"The vector sidecar '{sidecar.TableName}' cannot be written from here: {sidecar.Unavailable}");

        // Read every embedding first: the write below runs in one transaction, and streaming pages of
        // documents through the same connection while it is open is not something every driver allows.
        var vectors = new List<(string Id, float[] Values)>();
        var scanned = 0;

        var query = new BrowseQuery { Sort = new BrowseSort("Id", false, true) };
        await foreach (var row in this.StreamAll(profileId, table, typeName, query, ct))
        {
            if (scanned >= limit)
                break;

            scanned++;
            if (VectorMath.Read(row.Read(path)) is { } values)
                vectors.Add((row.Id, values));
        }

        var provider = connection.Provider;
        var safeTable = Ado.SafeIdentifier(table);
        var orphans = await this.ReadSidecarIds(profileId, table, typeName, ct) ?? [];
        orphans.ExceptWith(vectors.Select(v => v.Id));

        var written = await connection.Execute(async (db, token) =>
        {
            await using var transaction = await db.BeginTransactionAsync(token);

            foreach (var id in orphans)
                await DeleteVectorRow(provider, db, transaction, safeTable, typeName, id, token);

            var count = 0;
            foreach (var (id, values) in vectors)
            {
                await UpsertVectorRow(provider, db, transaction, safeTable, typeName, id, values, token);
                count++;
            }

            await transaction.CommitAsync(token);
            return count;
        }, ct);

        logger.LogInformation(
            "Rebuilt vector sidecar {Sidecar}: {Written} row(s) written, {Removed} orphan(s) removed",
            sidecar.TableName, written, orphans.Count);

        this.vectorSidecarCache.TryRemove($"{profileId}|{table}|{typeName}", out _);
        return written;
    }

    /// <summary>
    /// Replaces one document's embedding, or removes it when <paramref name="values"/> is null. The
    /// body and the sidecar move together, so the two cannot be left disagreeing by this path.
    /// </summary>
    public async Task WriteVector(
        string profileId,
        string table,
        string typeName,
        string id,
        string path,
        float[]? values,
        CancellationToken ct = default)
    {
        var connection = await this.Connect(profileId, ct);
        connection.AssertWritable();

        // Refused up front rather than half-done: without a writable sidecar this would remove the
        // embedding from the body and leave the index still returning the document for it.
        var sidecar = await this.DescribeVectorSidecar(profileId, table, typeName, refresh: false, ct);
        if (sidecar.Present && !sidecar.Maintainable)
            throw new InvalidOperationException(
                $"The vector sidecar '{sidecar.TableName}' cannot be written from here, so the body and the " +
                $"index would end up disagreeing: {sidecar.Unavailable}");

        var row = await this.GetDocument(profileId, table, typeName, id, ct)
                  ?? throw new InvalidOperationException($"No '{typeName}' document with id '{id}' exists any more.");

        if (JsonNode.Parse(row.Json) is not JsonObject body)
            throw new JsonException("A document must be a JSON object.");

        var segments = path.Split('.', StringSplitOptions.RemoveEmptyEntries);
        if (segments.Length == 0)
            throw new ArgumentException("A JSON path is required.", nameof(path));

        var target = body;
        for (var i = 0; i < segments.Length - 1; i++)
        {
            target = target[segments[i]] as JsonObject
                     ?? throw new InvalidOperationException($"'{path}' does not exist on document '{id}'.");
        }

        var leaf = segments[^1];
        if (values is null)
            target[leaf] = null;
        else
            target[leaf] = new JsonArray([.. values.Select(v => JsonValue.Create(v))]);

        // A replacement rides along with SaveDocument, which re-indexes the one embedding it finds in
        // the body it writes.
        await this.SaveDocument(profileId, table, typeName, id, body.ToJsonString(Compact), isNew: false, ct: ct);

        // A removal does not: the body no longer holds an embedding, so SaveDocument had nothing to
        // sync and deliberately left the sidecar alone. Dropping the row is this method's job.
        if (values is null && sidecar.Maintainable)
        {
            var safeTable = Ado.SafeIdentifier(table);
            await connection.Execute(
                (db, token) => DeleteVectorRow(connection.Provider, db, null, safeTable, typeName, id, token),
                ct);
        }

        logger.LogInformation(
            "{Action} vector at {Path} on {Type}/{Id}",
            values is null ? "Cleared" : "Replaced", path, typeName, id);
    }

    // ── Sidecar DML, for callers that already own a connection ──────────

    static async Task UpsertVectorRow(
        IDatabaseProvider provider,
        DbConnection db,
        DbTransaction? transaction,
        string safeTable,
        string typeName,
        string id,
        float[] values,
        CancellationToken ct)
    {
        var mapping = SyntheticMapping(values.Length);
        var sql = provider.BuildVectorUpsertSql(safeTable, typeName, mapping);
        if (sql is null)
            return;

        await using var cmd = Ado.Command(db, sql);
        cmd.Transaction = transaction;
        Ado.Bind(cmd, "@vecDocId", id);
        Ado.Bind(cmd, "@vecTypeName", typeName);
        Ado.Bind(cmd, "@embedding", provider.FormatVectorParameter(values, mapping));
        await cmd.ExecuteNonQueryAsync(ct);
    }

    static async Task DeleteVectorRow(
        IDatabaseProvider provider,
        DbConnection db,
        DbTransaction? transaction,
        string safeTable,
        string typeName,
        string id,
        CancellationToken ct)
    {
        var sql = provider.BuildVectorDeleteSql(safeTable, typeName);
        if (sql is null)
            return;

        await using var cmd = Ado.Command(db, sql);
        cmd.Transaction = transaction;
        Ado.Bind(cmd, "@vecDocId", id);
        if (sql.Contains("@vecTypeName", StringComparison.Ordinal))
            Ado.Bind(cmd, "@vecTypeName", typeName);

        await cmd.ExecuteNonQueryAsync(ct);
    }

    static async Task ClearVectorRows(
        IDatabaseProvider provider,
        DbConnection db,
        DbTransaction? transaction,
        string safeTable,
        string typeName,
        CancellationToken ct)
    {
        var sql = provider.BuildVectorClearSql(safeTable, typeName);
        if (sql is null)
            return;

        await using var cmd = Ado.Command(db, sql);
        cmd.Transaction = transaction;
        if (sql.Contains("@vecTypeName", StringComparison.Ordinal))
            Ado.Bind(cmd, "@vecTypeName", typeName);

        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// The single embedding in a document body, when there is exactly one. More than one candidate is
    /// reported as none: a type has one vector mapping and therefore one sidecar, and writing the
    /// wrong array into it is worse than writing nothing and saying so.
    /// </summary>
    public static float[]? SoleVector(string json)
    {
        if (Parse(json) is not JsonObject obj)
            return null;

        float[]? found = null;
        foreach (var (path, _) in ScanForVectors(obj, prefix: ""))
        {
            if (found is not null)
                return null;

            found = VectorMath.Read(obj.Read(path));
        }

        return found;
    }

    /// <summary>
    /// Stands in for the <c>VectorMapping</c> that <c>MapVectorProperty&lt;T&gt;</c> would have
    /// produced. An admin tool has no CLR type to read one off, and does not need the whole record:
    /// of everything on it, only <c>Dimensions</c> reaches the upsert SQL - DuckDB and SQL Server cast
    /// the bound literal to <c>FLOAT[n]</c> / <c>VECTOR(n)</c> - and the dimension is exactly what the
    /// document itself tells us. Metric and index kind shape only the DDL and the search SQL, neither
    /// of which this path issues. A dimension that disagrees with the column is rejected by the
    /// engine, which is the behaviour worth having.
    /// </summary>
    static VectorMapping SyntheticMapping(int dimensions) => new()
    {
        DocumentType = typeof(object),
        PropertyName = "embedding",
        JsonPath = "embedding",
        Dimensions = dimensions,
        Metric = VectorDistance.Cosine,
        IndexKind = VectorIndexKind.None,
        IndexOptions = new VectorIndexOptions(),
        GetVector = _ => ReadOnlyMemory<float>.Empty,
        SetVector = (_, _) => { }
    };
}

static class JsonPathExtensions
{
    /// <summary>Reads a dotted path out of a parsed body, the counterpart to <c>DocumentRow.Read</c>.</summary>
    public static JsonNode? Read(this JsonObject obj, string path)
    {
        JsonNode? node = obj;
        foreach (var segment in path.Split('.', StringSplitOptions.RemoveEmptyEntries))
        {
            if (node is not JsonObject current || !current.TryGetPropertyValue(segment, out var next))
                return null;

            node = next;
        }

        return node;
    }
}
