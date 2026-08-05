using System.Text.Json;
using System.Text.Json.Nodes;
using ShinyDocDbMyAdmin.Models;
using Shiny.DocumentDb;

namespace ShinyDocDbMyAdmin.Services;

/// <summary>
/// Thrown when a write would replace an encryption envelope with clear text.
/// </summary>
/// <remarks>
/// The failure this prevents is not an exception, which is what makes it worth an exception here: the
/// library's converter treats a non-envelope as pre-encryption plaintext and hands it back as-is, so a
/// document edited this way keeps working and is simply no longer protected. Nothing downstream ever
/// notices. That is the single most damaging thing this tool is capable of, so it is refused by default
/// and can only proceed on an explicit, per-write decision.
/// </remarks>
public sealed class EncryptedFieldDowngradeException(IReadOnlyList<string> paths)
    : InvalidOperationException(BuildMessage(paths))
{
    /// <summary>The dotted paths that hold an envelope now and would hold clear text after the write.</summary>
    public IReadOnlyList<string> Paths { get; } = paths;

    static string BuildMessage(IReadOnlyList<string> paths)
        => $"{string.Join(", ", paths)} would be saved in clear text. The application will read " +
           (paths.Count == 1 ? "it" : "them") +
           " back as plaintext and the value is no longer protected.";
}

/// <summary>
/// Key coverage across a whole type — the one encryption question only an admin tool can answer.
/// </summary>
/// <remarks>
/// <para>
/// The library gives you <c>RewrapAsync&lt;T&gt;()</c> to move documents onto a new key, but nothing tells
/// you whether it finished. Retiring a key while documents are still under it makes those documents
/// unreadable, and there is no way to find that out from the application side without reading every
/// document. Counting them in the database is cheap by comparison and is exactly what this does.
/// </para>
/// <para>
/// Nothing here decrypts. The counts come from the envelope's own <c>enc:1:&lt;keyId&gt;:</c> prefix, matched
/// with <c>LIKE</c> over the provider's JSON extract — portable across all nine dialects using the helpers
/// the browse grid already relies on, where a portable <c>SUBSTRING</c> over a JSON extract is not.
/// </para>
/// </remarks>
public sealed partial class DocumentAdminService
{
    public async Task<EncryptionInventory> DescribeEncryption(
        string profileId,
        string table,
        string typeName,
        CancellationToken ct = default)
    {
        // Which paths are encrypted, and which keys to count, both come from the sampled schema: the
        // database cannot be asked "which fields hold envelopes" without reading every document.
        var schema = await this.InferSchema(profileId, table, typeName, ct: ct);
        var encrypted = schema.Fields.Where(f => f.Encryption is not null).ToList();

        if (encrypted.Count == 0)
            return new EncryptionInventory(typeName, schema.SampleSize, []);

        var connection = await this.Connect(profileId, ct);
        var provider = connection.Provider;
        var quoted = provider.QuoteTable(Ado.SafeIdentifier(table));

        var stats = await connection.Execute(async (db, token) =>
        {
            var results = new List<EncryptedPathStats>(encrypted.Count);

            foreach (var field in encrypted)
            {
                results.Add(await CountPath(db, provider, quoted, typeName, field, token));
            }

            return results;
        }, ct);

        return new EncryptionInventory(typeName, schema.SampleSize, stats);
    }

    /// <summary>
    /// One scan per path rather than one per (path, key): every bucket is a conditional aggregate over the
    /// same row set, and <c>SUM(CASE WHEN … THEN 1 ELSE 0 END)</c> is spelled identically in all nine
    /// dialects. The scan itself is unavoidable — an encrypted path is rarely indexed, and where it is the
    /// index is on the ciphertext, not on its prefix.
    /// </summary>
    static async Task<EncryptedPathStats> CountPath(
        System.Data.Common.DbConnection db,
        IDatabaseProvider provider,
        string quotedTable,
        string typeName,
        InferredField field,
        CancellationToken ct)
    {
        var expr = ColumnExpression(provider, field.Path);
        var keyIds = field.Encryption!.KeyIds;

        var buckets = new List<string>
        {
            "COUNT(*)",
            // Anything that looks like an envelope at all, so the plaintext count is total minus this and
            // the Other bucket is "an envelope under a key the sample never saw".
            $"SUM(CASE WHEN {Like(expr, "@p_any")} THEN 1 ELSE 0 END)"
        };

        buckets.AddRange(keyIds.Select((_, i) => $"SUM(CASE WHEN {Like(expr, $"@p_{i}")} THEN 1 ELSE 0 END)"));

        var sql = $"SELECT {string.Join(", ", buckets)} FROM {quotedTable} " +
                  $"WHERE TypeName = @typeName AND {provider.JsonNullCheck("Data", field.Path, false)}";

        await using var cmd = Ado.Command(db, sql);
        Ado.Bind(cmd, "@typeName", typeName);
        Ado.Bind(cmd, "@p_any", $"{provider.EscapeLikePattern(DocumentEncryptionFormat.Prefix)}%");

        for (var i = 0; i < keyIds.Count; i++)
        {
            // enc:1:<keyId>: — the version is pinned because that is the only version the format defines,
            // and a future one would legitimately land in the Other bucket rather than be miscounted here.
            var prefix = $"{DocumentEncryptionFormat.Prefix}1:{keyIds[i]}:";
            Ado.Bind(cmd, $"@p_{i}", $"{provider.EscapeLikePattern(prefix)}%");
        }

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
            return new EncryptedPathStats(field.Path, 0, 0, new Dictionary<string, long>(), 0, field.Encryption.DeterministicObserved);

        var total = Ado.Count(reader.GetValue(0));
        var envelopes = Ado.Count(reader.GetValue(1));

        var byKeyId = new Dictionary<string, long>(StringComparer.Ordinal);
        for (var i = 0; i < keyIds.Count; i++)
            byKeyId[keyIds[i]] = Ado.Count(reader.GetValue(2 + i));

        var known = byKeyId.Values.Sum();

        return new EncryptedPathStats(
            field.Path,
            total,
            Plaintext: total - envelopes,
            byKeyId,
            OtherKeys: envelopes - known,
            field.Encryption.DeterministicObserved);

        string Like(string column, string parameter)
            => $"{column} LIKE {parameter}{provider.LikeEscapeClause}";
    }

    /// <summary>
    /// What to say before indexing an encrypted path, or null when there is nothing to say.
    /// </summary>
    /// <remarks>
    /// A warning rather than a refusal, in both directions. Indexing a <b>randomized</b> path is dead weight -
    /// every value is distinct and no predicate can name one - but a <b>deterministic</b> path is legitimately
    /// indexable, that is the point of the mode, and the sample can only ever prove one of the two. Blocking
    /// on a guess would stop the useful case to prevent a wasteful one.
    /// </remarks>
    public static string? IndexWarningFor(InferredSchema schema, IReadOnlyList<string> jsonPaths)
    {
        var encrypted = schema.Fields
            .Where(f => f.Encryption is not null && jsonPaths.Contains(f.Path, StringComparer.OrdinalIgnoreCase))
            .ToList();

        if (encrypted.Count == 0)
            return null;

        var deterministic = encrypted.Where(f => f.Encryption!.DeterministicObserved).Select(f => f.Path).ToList();
        var unknown = encrypted.Where(f => !f.Encryption!.DeterministicObserved).Select(f => f.Path).ToList();

        if (unknown.Count == 0)
            return $"{string.Join(", ", deterministic)} is deterministically encrypted, so an index over it does " +
                   "work - equality against a matching ciphertext will use it.";

        return $"{string.Join(", ", unknown)} is encrypted and the sample could not prove the mode. If it is " +
               "randomized, every value is distinct and no predicate can name one, so the index is dead weight. " +
               "If it is deterministic, the index is worth having.";
    }

    // ── The write guard ─────────────────────────────────────────────────

    /// <summary>
    /// The paths a submitted body would unprotect: ones that hold an envelope in the stored document, or
    /// that the type's other documents hold an envelope at, and that the submission fills with clear text.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Two sources, because they catch different mistakes. The stored body catches the common one — open a
    /// document, edit it, save, and the base64 you overtyped was the protection. The type's sampled paths
    /// catch the other one: a <i>new</i> document, or a field that happened to be null in this document,
    /// written as clear text into a path every other document protects.
    /// </para>
    /// <para>
    /// A byte-identical envelope is untouched and fine. So is removing the field or setting it to null —
    /// that deletes a value rather than exposing one.
    /// </para>
    /// </remarks>
    public static IReadOnlyList<string> FindEncryptionDowngrades(
        JsonNode? storedBody,
        JsonNode? submittedBody,
        IReadOnlyList<string> typeEncryptedPaths)
    {
        var candidates = new List<string>(EncryptedFields.PathsIn(storedBody));
        foreach (var path in typeEncryptedPaths)
        {
            if (!candidates.Contains(path, StringComparer.Ordinal))
                candidates.Add(path);
        }

        var downgraded = new List<string>();
        foreach (var path in candidates)
        {
            var value = EncryptedFields.Read(submittedBody, path);
            var present = value is not null && value.GetValueKind() != JsonValueKind.Null;

            if (present && !EncryptedFields.TryRead(value, out _))
                downgraded.Add(path);
        }

        return downgraded;
    }

    /// <summary>
    /// Everything a write path needs to decide whether it is about to unprotect something: the type's
    /// encrypted paths, and the stored body when one exists.
    /// </summary>
    public async Task<IReadOnlyList<string>> EncryptedPathsFor(string profileId, string table, string typeName, CancellationToken ct = default)
    {
        // Sampled per write rather than cached. A guard that can be stale is a guard that fails open, and
        // this is one 200-row read on an interactive, single-document operation - the same read the browse
        // grid already makes when it renders a page of the same type.
        var schema = await this.InferSchema(profileId, table, typeName, ct: ct);
        return EncryptedPaths(schema);
    }
}

/// <summary>Key coverage on one encrypted path, counted across every document of the type.</summary>
/// <param name="Total">Documents of the type whose body carries a non-null value at this path.</param>
/// <param name="Plaintext">
/// Values that are not envelopes. Usually documents written before the property was mapped;
/// <c>RewrapAsync</c> converts them.
/// </param>
/// <param name="ByKeyId">Counts per key id the sample found, keyed by id.</param>
/// <param name="OtherKeys">
/// Envelopes under a key id the 200-document sample never saw. Self-correcting: it goes to zero as soon as
/// the sample is wide enough, and a non-zero value is the signal to widen it or check the key ring.
/// </param>
public sealed record EncryptedPathStats(
    string Path,
    long Total,
    long Plaintext,
    IReadOnlyDictionary<string, long> ByKeyId,
    long OtherKeys,
    bool DeterministicObserved)
{
    /// <summary>True when every value on this path is an envelope under one known key.</summary>
    public bool RotationComplete => this.ByKeyId.Count <= 1 && this.OtherKeys == 0 && this.Plaintext == 0;

    /// <summary>The key holding the most values, which is what a rotation is moving everything onto.</summary>
    public string? DominantKeyId => this.ByKeyId.Count == 0
        ? null
        : this.ByKeyId.OrderByDescending(x => x.Value).First().Key;

    /// <summary>
    /// The operational verdict in one sentence, because that is the whole value of the card — the counts
    /// are evidence for it, not the point of it.
    /// </summary>
    public string Verdict
    {
        get
        {
            if (this.Plaintext > 0)
                return $"{this.Plaintext:N0} of {this.Total:N0} values are not encrypted. They were written before the property was mapped; RewrapAsync converts them.";

            if (this.OtherKeys > 0)
                return $"{this.OtherKeys:N0} values are under a key not seen in the sample. Widen the sample or check the key ring.";

            if (this.ByKeyId.Count > 1)
            {
                var dominant = this.DominantKeyId!;
                var stragglers = this.ByKeyId.Where(x => x.Key != dominant).Sum(x => x.Value);
                var older = string.Join(", ", this.ByKeyId.Keys.Where(k => k != dominant).Order(StringComparer.Ordinal));
                return $"{stragglers:N0} of {this.Total:N0} are still under {older}. Run RewrapAsync<T>() before retiring {(this.ByKeyId.Count > 2 ? "those keys" : older)}.";
            }

            return this.Total == 0
                ? "No values on this path."
                : $"{this.Total:N0} values, all under {this.DominantKeyId}. Nothing left under an older key.";
        }
    }
}

/// <summary>
/// Key coverage for a whole type. <paramref name="SampleSize"/> is how many documents were read to decide
/// which paths are encrypted and which keys to count — the counts themselves are over every document.
/// </summary>
public sealed record EncryptionInventory(string TypeName, int SampleSize, IReadOnlyList<EncryptedPathStats> Paths)
{
    public bool IsEmpty => this.Paths.Count == 0;

    /// <summary>True when nothing on this type needs a rewrap.</summary>
    public bool RotationComplete => this.Paths.All(p => p.RotationComplete);
}
