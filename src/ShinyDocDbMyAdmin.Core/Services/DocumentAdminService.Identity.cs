using System.Data.Common;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using Shiny.DocumentDb;
using ShinyDocDbMyAdmin.Models;

namespace ShinyDocDbMyAdmin.Services;

/// <summary>
/// Deciding what a database <b>is</b>: which of its tables are DocumentDb's, which belong to something
/// else, and whether the database participates at all.
/// </summary>
/// <remarks>
/// <para>
/// This replaced a classifier that ran name-substring rules first and probed the database second. Both
/// halves were wrong. The substring rules are unanchored, so a business table called <c>audit_history</c>
/// or <c>geo_spatial_index</c> was reported as a DocumentDb sidecar, and a documents table called
/// <c>orders_history</c> never reached the probe at all - it was classified as history and vanished from
/// the explorer, the filter console and the assistant. The probe was a failing
/// <c>SELECT … WHERE 1 = 0</c> per table, which costs a <c>DbException</c> per foreign table (~300 of them
/// in a shared schema) and cannot run inside a transaction on PostgreSQL.
/// </para>
/// <para>
/// What replaces it is evidence. Two catalog reads describe the whole database - the table list and every
/// column of every table, with types. A table is a <i>candidate</i> when it carries all five envelope
/// columns, and it is <see cref="TableConfidence.Confirmed"/> only when something DocumentDb leaves behind
/// corroborates it. Sidecars are then found by <b>computing the names DocumentDb would have created</b>
/// (<see cref="IDatabaseProvider.OwnedTableNames"/>) and looking them up: name matching becomes
/// confirmation of a name we derived rather than a guess at one we found. Everything left over is foreign,
/// whatever it is called.
/// </para>
/// </remarks>
public sealed partial class DocumentAdminService
{
    /// <summary>Rows read per document table when a caller asks for the opt-in data signal.</summary>
    const int IdentitySampleRows = 5;

    /// <summary>Column types that can hold a JSON document across the nine dialects.</summary>
    static readonly string[] JsonColumnTypes =
        ["json", "jsonb", "text", "clob", "nclob", "varchar", "nvarchar", "char", "string", "longtext", "mediumtext"];

    /// <summary>
    /// A primary key or unique index over <c>(Id, TypeName)</c> as the catalogs spell it. Word-boundary
    /// matched, because an index <i>definition</i> mentioning <c>idx_json_…</c> contains "id" as a
    /// substring and would otherwise pass.
    /// </summary>
    static readonly Regex EnvelopeKeyDefinition =
        new(@"\bunique\b(?=.*\bid\b)(?=.*\btypename\b)", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    /// <summary>
    /// The verdict for a whole database. Computed from the same catalog read that classifies the tables,
    /// so a caller that has already listed them pays nothing for asking.
    /// </summary>
    /// <param name="sampleRows">
    /// Reads a handful of rows from each candidate table, so a table with the envelope but no index can
    /// still be confirmed by what is <i>in</i> it, and so field-level encryption shows up in the feature
    /// list. Off by default: it is the only part of this that reads data rather than catalog.
    /// </param>
    public async Task<DatabaseIdentity> GetIdentity(
        string profileId,
        bool sampleRows = false,
        bool refresh = false,
        CancellationToken ct = default)
        => (await this.Catalog(profileId, refresh, sampleRows, ct)).Identity;

    /// <summary>The classified tables, cached per profile until <see cref="InvalidateSchema"/>.</summary>
    public async Task<IReadOnlyList<TableInfo>> ListTables(string profileId, bool refresh = false, CancellationToken ct = default)
        => (await this.Catalog(profileId, refresh, sampleRows: false, ct)).Tables;

    async Task<CatalogSnapshot> Catalog(string profileId, bool refresh, bool sampleRows, CancellationToken ct)
    {
        // A sampled snapshot is strictly better informed than an unsampled one, so it satisfies a caller
        // that did not ask for sampling - but not the other way round.
        if (!refresh && this.catalogCache.TryGetValue(profileId, out var cached) && (cached.Sampled || !sampleRows))
            return cached;

        var connection = await this.Connect(profileId, ct);
        var snapshot = await connection.Execute(
            (db, token) => Classify(db, connection, sampleRows, token), ct);

        this.catalogCache[profileId] = snapshot;
        return snapshot;
    }

    /// <summary>What one catalog read produced: every table classified, and the database's own verdict.</summary>
    sealed record CatalogSnapshot(IReadOnlyList<TableInfo> Tables, DatabaseIdentity Identity, bool Sampled);

    static async Task<CatalogSnapshot> Classify(DbConnection db, AdminConnection connection, bool sampleRows, CancellationToken ct)
    {
        var provider = connection.Provider;

        var names = await ReadTableNames(db, provider, ct);
        var catalog = await ReadColumns(db, provider, ct);

        // ── Which tables carry the envelope ─────────────────────────────────
        var candidates = names.Where(n => catalog.TryGetValue(n, out var c) && HasEnvelope(c)).ToList();

        // Carrying the envelope is not enough on its own, and the blob sidecar is the proof: it is
        // Id / TypeName / Data / CreatedAt / UpdatedAt plus BlobKey, so by columns alone it reads as a
        // documents table. A candidate that is a name another candidate would have created is that other
        // table's sidecar, not a documents table of its own. Only the type-independent names are needed
        // here - no per-type sidecar carries the envelope - which is what keeps this from needing the type
        // lists it would otherwise have to read first.
        var sidecarNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var candidate in candidates)
        {
            foreach (var sidecar in provider.OwnedTableNames(candidate, null))
            {
                if (!sidecar.Equals(candidate, StringComparison.OrdinalIgnoreCase))
                    sidecarNames.Add(sidecar);
            }
        }

        var documents = new List<DocumentTable>();
        foreach (var name in candidates.Where(c => !sidecarNames.Contains(c)))
        {
            var columns = catalog[name];
            var evidence = await Evidence(db, provider, name, columns, sampleRows, ct);
            documents.Add(new DocumentTable(
                name,
                columns.ContainsKey("TenantId"),
                columns,
                evidence,
                await ReadTypeNames(db, provider, name, ct)));
        }

        // ── The sidecars, by computed name rather than by substring ─────────
        var owned = new Dictionary<string, OwnedTable>(StringComparer.OrdinalIgnoreCase);
        foreach (var table in documents)
        {
            foreach (var candidate in OwnedNames(provider, table))
            {
                // First owner wins. Two document tables can only collide here by being named such that one
                // is the other's sidecar prefix, and the shorter name is the one the convention derives from.
                if (!owned.ContainsKey(candidate) && !documents.Any(d => d.Name.Equals(candidate, StringComparison.OrdinalIgnoreCase)))
                    owned[candidate] = new OwnedTable(table.Name, OwnedRole(provider, table.Name, candidate));
            }
        }

        var tables = new List<TableInfo>(names.Count);
        foreach (var name in names)
        {
            var document = documents.FirstOrDefault(d => d.Name.Equals(name, StringComparison.Ordinal));
            if (document is not null)
            {
                tables.Add(new TableInfo(
                    name,
                    TableRole.Documents,
                    document.HasTenant,
                    Owner: null,
                    document.Evidence.Confidence,
                    document.Evidence.Confidence == TableConfidence.Probable
                        ? "envelope only - nothing here proves DocumentDb wrote it"
                        : null));
                continue;
            }

            tables.Add(owned.TryGetValue(name, out var sidecar)
                ? new TableInfo(name, sidecar.Role, false, sidecar.Owner, TableConfidence.Confirmed, Detail(sidecar.Owner, name, sidecar.Role))
                : new TableInfo(name, TableRole.Foreign));
        }

        return new CatalogSnapshot(tables, Verdict(connection, tables, documents), sampleRows);
    }

    // ── Reads ───────────────────────────────────────────────────────────

    static async Task<IReadOnlyList<string>> ReadTableNames(DbConnection db, IDatabaseProvider provider, CancellationToken ct)
    {
        var names = new List<string>();
        await using var cmd = Ado.Command(db, provider.BuildListTablesSql());
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            names.Add(Ado.Text(reader, 0));

        return [.. names.OrderBy(x => x, StringComparer.OrdinalIgnoreCase)];
    }

    /// <summary>
    /// Every column of every table, as one read. The table list is read separately rather than derived from
    /// this, because a provider's column view can legitimately not cover everything the table list does -
    /// SQLite's excludes virtual tables, whose columns cannot be read without the module that created them
    /// loaded. Such a table simply has no columns here, which is exactly right: it has no envelope, and it
    /// is still recognised by name if it is one we created.
    /// </summary>
    static async Task<Dictionary<string, Dictionary<string, string>>> ReadColumns(
        DbConnection db, IDatabaseProvider provider, CancellationToken ct)
    {
        var catalog = new Dictionary<string, Dictionary<string, string>>(StringComparer.OrdinalIgnoreCase);

        await using var cmd = Ado.Command(db, provider.BuildListColumnsSql());
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var table = Ado.Text(reader, 0);
            if (!catalog.TryGetValue(table, out var columns))
                catalog[table] = columns = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            columns[Ado.Text(reader, 1)] = Ado.NullableText(reader, 2) ?? "";
        }

        return catalog;
    }

    /// <summary>The distinct stored types in a documents table - what the per-type sidecar names are derived from.</summary>
    static async Task<IReadOnlyList<string>> ReadTypeNames(DbConnection db, IDatabaseProvider provider, string table, CancellationToken ct)
    {
        var quoted = provider.QuoteTable(Ado.SafeIdentifier(table));
        var types = new List<string>();

        await using var cmd = Ado.Command(db, $"SELECT DISTINCT TypeName FROM {quoted}");
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var type = Ado.NullableText(reader, 0);
            if (!string.IsNullOrEmpty(type))
                types.Add(type);
        }

        return types;
    }

    // ── Evidence ────────────────────────────────────────────────────────

    static bool HasEnvelope(Dictionary<string, string> columns)
        => columns.ContainsKey("Id")
        && columns.ContainsKey("TypeName")
        && columns.ContainsKey("Data")
        && columns.ContainsKey("CreatedAt")
        && columns.ContainsKey("UpdatedAt");

    /// <summary>
    /// Scores a candidate. The envelope is required but never sufficient - any table with those five column
    /// <i>names</i> has it, whatever the types and whatever is in it. What separates a DocumentDb table
    /// from a coincidence is the rest: a JSON-shaped <c>Data</c> column, the key the library declares, the
    /// indexes it creates, and - when the caller asks for it - a row that actually holds a document.
    /// </summary>
    static async Task<TableEvidence> Evidence(
        DbConnection db,
        IDatabaseProvider provider,
        string table,
        Dictionary<string, string> columns,
        bool sampleRows,
        CancellationToken ct)
    {
        var signals = new List<string>();

        var dataType = columns["Data"];
        if (JsonColumnTypes.Any(t => dataType.Contains(t, StringComparison.OrdinalIgnoreCase)))
            signals.Add($"Data is {dataType.ToLowerInvariant()}");

        foreach (var signal in await IndexSignals(db, provider, table, ct))
            signals.Add(signal);

        if (!sampleRows)
            return new TableEvidence(signals);

        var sample = await Sample(db, provider, table, ct);
        if (sample.HoldsDocuments)
            signals.Add("holds documents");

        return new TableEvidence(signals) { Encrypted = sample.Encrypted };
    }

    static async Task<IReadOnlyList<string>> IndexSignals(DbConnection db, IDatabaseProvider provider, string table, CancellationToken ct)
    {
        var sql = provider.BuildListAllIndexesSql(Ado.SafeIdentifier(table));
        if (sql is null)
            return [];

        var signals = new List<string>();
        var typeNameIndex = $"idx_{table}_typename";
        var jsonIndexes = 0;

        try
        {
            await using var cmd = Ado.Command(db, sql);
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                var name = Ado.Text(reader, 0);
                var definition = Ado.NullableText(reader, 1);

                if (name.Equals(typeNameIndex, StringComparison.OrdinalIgnoreCase))
                    signals.Add($"{typeNameIndex} present");
                else if (name.StartsWith(IndexPrefix, StringComparison.OrdinalIgnoreCase))
                    jsonIndexes++;
                else if (IsEnvelopeKey(table, name, definition))
                    signals.Add("keyed on (Id, TypeName)");
            }
        }
        catch (DbException)
        {
            // An index view a connection is not granted, or a catalog that disagrees with its own docs. The
            // classification does not depend on this - it only corroborates - so an unreadable index list
            // costs a signal, not an answer.
            return signals;
        }

        if (jsonIndexes > 0)
            signals.Add($"{jsonIndexes} JSON property index(es)");

        return signals;
    }

    /// <summary>
    /// Whether an index is the <c>(Id, TypeName)</c> key the library declares. Every engine records it
    /// differently - some name it after the table, some only list its columns - and SQLite and DuckDB do not
    /// surface it at all, which is why its absence proves nothing.
    /// </summary>
    static bool IsEnvelopeKey(string table, string name, string? definition)
        => name.Equals("PRIMARY", StringComparison.OrdinalIgnoreCase)
        || name.Equals($"PK_{table}", StringComparison.OrdinalIgnoreCase)
        || name.Equals($"{table}_pkey", StringComparison.OrdinalIgnoreCase)
        || (definition is { Length: > 0 } && EnvelopeKeyDefinition.IsMatch(definition));

    /// <summary>
    /// Reads a handful of rows: whether any of them looks like a stored document (parseable JSON under a
    /// non-empty type), and whether any field in them is a field-level encryption envelope. The one read
    /// here that touches data rather than catalog, which is why the caller has to ask for it.
    /// </summary>
    static async Task<SampleOutcome> Sample(DbConnection db, IDatabaseProvider provider, string table, CancellationToken ct)
    {
        var quoted = provider.QuoteTable(Ado.SafeIdentifier(table));
        var holdsDocuments = false;
        var encrypted = false;

        try
        {
            await using var cmd = Ado.Command(
                db, $"SELECT TypeName, Data FROM {quoted} {provider.BuildPaginationClause(0, IdentitySampleRows)}");

            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                if (string.IsNullOrEmpty(Ado.NullableText(reader, 0)))
                    continue;

                JsonNode? body;
                try
                {
                    body = JsonNode.Parse(Ado.Text(reader, 1));
                }
                catch (System.Text.Json.JsonException)
                {
                    // Not a document. Keep looking: one unparseable body does not settle the table.
                    continue;
                }

                if (body is not JsonObject)
                    continue;

                holdsDocuments = true;
                encrypted |= EncryptedFields.PathsIn(body).Count > 0;
            }
        }
        catch (DbException)
        {
            // The one read here that can fail on a table we do not own - a column of an incompatible type
            // behind a matching name. Absence of the signal is the answer.
        }

        return new SampleOutcome(holdsDocuments, encrypted);
    }

    sealed record SampleOutcome(bool HoldsDocuments, bool Encrypted);

    // ── Owned names ─────────────────────────────────────────────────────

    static IEnumerable<string> OwnedNames(IDatabaseProvider provider, DocumentTable table)
    {
        // Type-independent sidecars (history, blobs, spatial and whatever hangs off them) exist for a table
        // with no rows in it too, so they are asked for once with no type.
        foreach (var name in provider.OwnedTableNames(table.Name, null))
            yield return name;

        foreach (var type in table.Types)
        {
            foreach (var name in provider.OwnedTableNames(table.Name, type))
                yield return name;
        }
    }

    /// <summary>
    /// Which feature a computed name belongs to. Name matching is safe here in a way it is not in a
    /// classifier: these are names this provider just generated, so the match confirms a name we derived
    /// rather than guessing at one we found.
    /// </summary>
    static TableRole OwnedRole(IDatabaseProvider provider, string table, string name)
    {
        if (name.Equals(provider.HistoryTableName(table), StringComparison.OrdinalIgnoreCase))
            return TableRole.History;

        if (name.Equals(provider.BlobTableName(table), StringComparison.OrdinalIgnoreCase))
            return TableRole.Blobs;

        var suffix = name.Length > table.Length ? name[table.Length..] : "";
        if (suffix.StartsWith("_spatial", StringComparison.OrdinalIgnoreCase))
            return TableRole.Spatial;
        if (suffix.StartsWith("_vec", StringComparison.OrdinalIgnoreCase))
            return TableRole.Vector;

        return TableRole.FullText;
    }

    /// <summary>The half of a sidecar's identity the role cannot carry: which of several tables it is.</summary>
    static string? Detail(string owner, string name, TableRole role)
    {
        var suffix = name.Length > owner.Length ? name[owner.Length..] : "";

        return role switch
        {
            TableRole.Spatial when suffix.StartsWith("_spatial_map", StringComparison.OrdinalIgnoreCase) => "rowid map",
            TableRole.Spatial when !suffix.Equals("_spatial", StringComparison.OrdinalIgnoreCase) => "R*Tree shadow",
            TableRole.Vector when suffix.StartsWith("_vec_map", StringComparison.OrdinalIgnoreCase) => "document id map",
            TableRole.Vector when suffix.Contains("_chunks", StringComparison.OrdinalIgnoreCase)
                               || suffix.EndsWith("_rowids", StringComparison.OrdinalIgnoreCase)
                               || suffix.EndsWith("_info", StringComparison.OrdinalIgnoreCase) => "vec0 shadow",
            TableRole.FullText when suffix.StartsWith("_ftssrc", StringComparison.OrdinalIgnoreCase) => "indexed text",
            TableRole.FullText when !suffix.Equals("_fts", StringComparison.OrdinalIgnoreCase) => "FTS5 shadow",
            _ => null
        };
    }

    // ── The verdict ─────────────────────────────────────────────────────

    static DatabaseIdentity Verdict(AdminConnection connection, IReadOnlyList<TableInfo> tables, IReadOnlyList<DocumentTable> documents)
    {
        var foreign = tables.Count(t => t.Role == TableRole.Foreign);
        var sidecars = tables.Count(t => t.IsOwned && t.Role != TableRole.Documents);
        var types = documents.SelectMany(d => d.Types).Distinct(StringComparer.Ordinal).ToList();

        if (documents.Count == 0)
        {
            return new DatabaseIdentity(
                false, IdentityConfidence.None, 0, 0, 0, foreign, [],
                [$"{tables.Count} table(s) read; none carry the Id / TypeName / Data / CreatedAt / UpdatedAt envelope."]);
        }

        var confirmed = documents.Count(d => d.Evidence.Confidence == TableConfidence.Confirmed);
        var reasons = new List<string>
        {
            $"{documents.Count} table(s) carry the document envelope."
        };

        foreach (var table in documents.Where(d => d.Evidence.Signals.Count > 0))
            reasons.Add($"{table.Name}: {string.Join(", ", table.Evidence.Signals)}.");

        if (confirmed < documents.Count)
            reasons.Add($"{documents.Count - confirmed} of them carry the envelope and nothing else.");

        if (sidecars > 0)
            reasons.Add($"{sidecars} table(s) match names DocumentDb would have created.");

        if (foreign > 0)
            reasons.Add($"{foreign} table(s) belong to something else.");

        return new DatabaseIdentity(
            true,
            confirmed > 0 ? IdentityConfidence.Confirmed : IdentityConfidence.Probable,
            documents.Count,
            types.Count,
            sidecars,
            foreign,
            Features(connection, tables, documents, types),
            reasons);
    }

    /// <summary>
    /// What this database provably uses. Everything here is read from the catalog or from the type list -
    /// except soft delete, which no database records, and which therefore appears only when the operator
    /// has declared it on the connection.
    /// </summary>
    static IReadOnlyList<string> Features(
        AdminConnection connection,
        IReadOnlyList<TableInfo> tables,
        IReadOnlyList<DocumentTable> documents,
        IReadOnlyList<string> types)
    {
        var features = new List<string>();

        if (tables.Any(t => t.Role == TableRole.History)) features.Add("temporal");
        if (tables.Any(t => t.Role == TableRole.Blobs)) features.Add("blobs");
        if (tables.Any(t => t.Role == TableRole.Spatial)) features.Add("spatial");
        if (tables.Any(t => t.Role == TableRole.Vector)) features.Add("vectors");

        // Only two providers give full text a table of its own; the rest add a generated column to the
        // documents table, which the same catalog read already has.
        if (tables.Any(t => t.Role == TableRole.FullText) || documents.Any(HasFullTextColumn))
            features.Add("full text");

        if (types.Any(IsOutboxTypeName)) features.Add("outbox");
        if (documents.Any(d => d.HasTenant)) features.Add("tenant");
        if (documents.Any(d => d.Evidence.Encrypted)) features.Add("encryption");

        var declared = connection.Profile.Profile.SoftDeleteFlags;
        if (declared.Any(f => types.Contains(f.TypeName, StringComparer.Ordinal)))
            features.Add("soft delete");

        return features;
    }

    /// <summary>The generated column full text adds on every provider that does not build a table for it.</summary>
    static bool HasFullTextColumn(DocumentTable table)
        => table.Columns.Keys.Any(c =>
            c.StartsWith("fts_", StringComparison.OrdinalIgnoreCase) ||
            c.StartsWith("ftcc_", StringComparison.OrdinalIgnoreCase));

    // ── Working state ───────────────────────────────────────────────────

    sealed record DocumentTable(
        string Name,
        bool HasTenant,
        Dictionary<string, string> Columns,
        TableEvidence Evidence,
        IReadOnlyList<string> Types);

    sealed record OwnedTable(string Owner, TableRole Role);

    /// <summary>
    /// The signals found for one candidate. The envelope is the entry fee, so it is not in here; these are
    /// the things that make the difference between "carries those five columns" and "DocumentDb wrote this".
    /// </summary>
    sealed record TableEvidence(IReadOnlyList<string> Signals)
    {
        public TableConfidence Confidence => this.Signals.Count > 0 ? TableConfidence.Confirmed : TableConfidence.Probable;

        public bool Encrypted { get; init; }
    }
}
