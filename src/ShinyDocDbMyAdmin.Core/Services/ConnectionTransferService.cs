using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using ShinyDocDbMyAdmin.Models;

namespace ShinyDocDbMyAdmin.Services;

/// <summary>What importing a bundled connection would do to the current list.</summary>
public enum ImportAction
{
    /// <summary>Leave what is here alone. The default for anything that already exists.</summary>
    Skip,

    /// <summary>Write it as a new connection with a fresh id.</summary>
    Add,

    /// <summary>Overwrite the matching connection in place.</summary>
    Replace
}

/// <summary>One line of the import review: what is in the file, and what is already here.</summary>
public sealed record ImportCandidate(BundledConnection Connection, ConnectionProfile? Existing)
{
    public bool Conflicts => this.Existing is not null;
    public string Name => this.Connection.Name;

    /// <summary>Chosen for you, changed by you. Skip for anything that already exists.</summary>
    public ImportAction Suggested => this.Conflicts ? ImportAction.Skip : ImportAction.Add;
}

public sealed record ImportPreview(
    ConnectionBundle Bundle,
    IReadOnlyList<ImportCandidate> Candidates,
    bool NeedsPassphrase,
    string? Problem
);

public sealed record ImportOutcome(int Added, int Replaced, int Skipped, IReadOnlyList<string> Errors);

/// <summary>
/// Moving the connection list between instances.
/// </summary>
/// <remarks>
/// <para>
/// The interesting decision is what happens to secrets. Connection strings, SQLCipher passwords and
/// AI API keys are stored encrypted under <see cref="SecretProtector"/>'s instance key - which is
/// exactly the wrong key for an export, because it makes the file useless anywhere but the machine
/// that wrote it, the one place an export is not needed.
/// </para>
/// <para>
/// So: secrets are <b>left out by default</b>, and opting in re-encrypts them under a passphrase the
/// exporter types. The export is then a credential file, but a portable one that is inert without
/// the passphrase - and it took a deliberate act to make it.
/// </para>
/// </remarks>
public sealed class ConnectionTransferService(
    ProfileStore profiles,
    DemoMode demo,
    ILogger<ConnectionTransferService> logger)
{
    const string Prefix = "pbe:v1:";
    const int Iterations = 600_000;
    const string Derivation = "pbkdf2-sha256:600000";

    /// <summary>
    /// Indented and with enums as names.
    /// </summary>
    /// <remarks>
    /// The string enums matter more than the indenting. The default writes them as ordinals, which
    /// would make the bundle unreadable to whoever opens it - and, worse, silently wrong if a value
    /// were ever inserted into the middle of <c>ProviderKind</c>: an old file's <c>2</c> would then
    /// import as a different backend without anything noticing. A name cannot drift like that.
    /// </remarks>
    static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web)
    {
        WriteIndented = true,
        Converters = { new System.Text.Json.Serialization.JsonStringEnumConverter() }
    };

    // ── Export ──────────────────────────────────────────────────────────

    /// <summary>
    /// Builds the bundle. With no passphrase the secret fields come back null rather than empty, so
    /// an importer can tell "not exported" from "exported as blank".
    /// </summary>
    public async Task<ConnectionBundle> Export(string? passphrase, CancellationToken ct = default)
    {
        demo.AssertCanTransferSettings();

        var includeSecrets = !string.IsNullOrEmpty(passphrase);
        byte[]? key = null;
        string? salt = null;

        if (includeSecrets)
        {
            var saltBytes = RandomNumberGenerator.GetBytes(16);
            salt = Convert.ToBase64String(saltBytes);
            key = DeriveKey(passphrase!, saltBytes);
        }

        var bundle = new ConnectionBundle
        {
            IncludesSecrets = includeSecrets,
            KeyDerivation = includeSecrets ? Derivation : null,
            Salt = salt,
            Source = Environment.MachineName
        };

        // Host-provided connections are declared by an AppHost or by configuration, so they are not
        // this instance's to hand out - and importing one would produce a connection the target
        // could not edit or delete either.
        var ours = (await profiles.List(ct)).Where(p => !profiles.IsProvided(p.Id));

        foreach (var profile in ours)
        {
            var bundled = new BundledConnection
            {
                Id = profile.Id,
                Name = profile.Name,
                Provider = profile.Provider,
                ReadOnly = profile.ReadOnly,
                UploadedFileName = profile.UploadedFileName,
                ConnectionString = key is null ? null : Protect(profiles.RevealConnectionString(profile), key),
                Password = key is null || profiles.RevealPassword(profile) is not { Length: > 0 } password
                    ? null
                    : Protect(password, key)
            };

            if (await profiles.GetAiSettings(profile.Id, ct) is { } ai)
            {
                bundled.Ai = new BundledAiSettings
                {
                    Provider = ai.Provider,
                    Model = ai.Model,
                    Endpoint = ai.Endpoint,
                    Enabled = ai.Enabled,
                    AllowInsert = ai.AllowInsert,
                    AllowUpdate = ai.AllowUpdate,
                    AllowDelete = ai.AllowDelete,
                    ApiKey = key is null || profiles.RevealApiKey(ai) is not { Length: > 0 } apiKey
                        ? null
                        : Protect(apiKey, key)
                };
            }

            foreach (var query in await profiles.ListQueries(profile.Id, ct))
                bundled.SavedQueries.Add(new BundledQuery { Name = query.Name, Sql = query.Sql });

            bundle.Connections.Add(bundled);
        }

        logger.LogInformation(
            "Exported {Count} connection(s){Secrets}.",
            bundle.Connections.Count,
            includeSecrets ? " with passphrase-encrypted secrets" : " without secrets");

        return bundle;
    }

    public static string Serialize(ConnectionBundle bundle) => JsonSerializer.Serialize(bundle, Json);

    // ── Import ──────────────────────────────────────────────────────────

    /// <summary>
    /// Reads a bundle and works out what importing it would do, without writing anything. Problems
    /// come back as text rather than exceptions - a bad file is an ordinary thing to hand a form.
    /// </summary>
    public async Task<ImportPreview> Preview(string json, string? passphrase, CancellationToken ct = default)
    {
        demo.AssertCanTransferSettings();

        ConnectionBundle? bundle;
        try
        {
            bundle = JsonSerializer.Deserialize<ConnectionBundle>(json, Json);
        }
        catch (JsonException ex)
        {
            return new ImportPreview(new ConnectionBundle(), [], false, $"That file is not valid JSON: {ex.Message}");
        }

        if (bundle is null || bundle.Connections.Count == 0)
            return new ImportPreview(new ConnectionBundle(), [], false, "That file holds no connections.");

        if (bundle.Version > ConnectionBundle.CurrentVersion)
        {
            return new ImportPreview(bundle, [], false,
                $"That file was written by a newer version of this tool (format {bundle.Version}, this build reads {ConnectionBundle.CurrentVersion}).");
        }

        var needsPassphrase = bundle.IncludesSecrets && string.IsNullOrEmpty(passphrase);
        string? problem = null;

        if (bundle.IncludesSecrets && !needsPassphrase)
        {
            // Checked here rather than at write time: finding out the passphrase was wrong after
            // half the connections are in is worse than being told before any of them are.
            if (!this.TryUnlock(bundle, passphrase!, out _))
                problem = "That passphrase does not open this file.";
        }

        var existing = await profiles.List(ct);
        var candidates = bundle.Connections
            .Select(c => new ImportCandidate(c, Match(existing, c)))
            .ToList();

        return new ImportPreview(bundle, candidates, needsPassphrase, problem);
    }

    /// <summary>
    /// Matched on id first, then on name. The id catches a re-import onto the instance that wrote
    /// the file; the name catches the same connection set up by hand on both.
    /// </summary>
    static ConnectionProfile? Match(IReadOnlyList<ConnectionProfile> existing, BundledConnection bundled)
        => existing.FirstOrDefault(p => p.Id == bundled.Id)
           ?? existing.FirstOrDefault(p => p.Name.Equals(bundled.Name, StringComparison.OrdinalIgnoreCase));

    /// <summary>Applies the decisions taken on the review screen.</summary>
    public async Task<ImportOutcome> Import(
        ConnectionBundle bundle,
        IReadOnlyDictionary<string, ImportAction> decisions,
        string? passphrase,
        CancellationToken ct = default)
    {
        demo.AssertCanTransferSettings();

        byte[]? key = null;
        if (bundle.IncludesSecrets)
        {
            if (!this.TryUnlock(bundle, passphrase ?? "", out key))
                return new ImportOutcome(0, 0, 0, ["That passphrase does not open this file."]);
        }

        var existing = await profiles.List(ct);
        var errors = new List<string>();
        int added = 0, replaced = 0, skipped = 0;

        foreach (var bundled in bundle.Connections)
        {
            var action = decisions.GetValueOrDefault(bundled.Id, ImportAction.Skip);

            if (action == ImportAction.Skip)
            {
                skipped++;
            }
            else
            {
                try
                {
                    var target = action == ImportAction.Replace ? Match(existing, bundled) : null;

                    if (this.Refuse(action, target, bundled) is { } refusal)
                    {
                        errors.Add(refusal);
                        skipped++;
                    }
                    else
                    {
                        await this.Write(bundled, target, key, ct);

                        if (target is null)
                            added++;
                        else
                            replaced++;
                    }
                }
                catch (Exception ex)
                {
                    // One bad connection should not abandon the rest of the file.
                    errors.Add($"'{bundled.Name}': {ex.Message}");
                }
            }
        }

        logger.LogInformation(
            "Imported connections: {Added} added, {Replaced} replaced, {Skipped} skipped.",
            added, replaced, skipped);

        return new ImportOutcome(added, replaced, skipped, errors);
    }

    /// <summary>Why this connection cannot be imported as asked, or null when it can.</summary>
    string? Refuse(ImportAction action, ConnectionProfile? target, BundledConnection bundled)
    {
        if (action == ImportAction.Replace && target is null)
            return $"'{bundled.Name}': nothing here to replace, so it was skipped.";

        if (target is not null && profiles.IsProvided(target.Id))
            return $"'{bundled.Name}' matches a connection declared by the host, which cannot be overwritten.";

        return null;
    }

    /// <summary>Writes one bundled connection, plus its assistant settings and saved queries.</summary>
    async Task Write(BundledConnection bundled, ConnectionProfile? target, byte[]? key, CancellationToken ct)
    {
        var profile = target ?? new ConnectionProfile();
        profile.Name = bundled.Name;
        profile.Provider = bundled.Provider;
        profile.ReadOnly = bundled.ReadOnly;

        // The uploaded file itself is not in the bundle - it is a database, not a setting - so the
        // reference is dropped rather than left pointing at nothing.
        profile.UploadedFileName = null;

        await profiles.Save(
            profile,
            key is null ? "" : Reveal(bundled.ConnectionString, key),
            key is null ? null : Reveal(bundled.Password, key),
            ct);

        if (bundled.Ai is { } ai)
        {
            var settings = await profiles.GetAiSettings(profile.Id, ct)
                           ?? new AiConnectionSettings { ProfileId = profile.Id };

            settings.Provider = ai.Provider;
            settings.Model = ai.Model;
            settings.Endpoint = ai.Endpoint;
            settings.Enabled = ai.Enabled;
            settings.AllowInsert = ai.AllowInsert;
            settings.AllowUpdate = ai.AllowUpdate;
            settings.AllowDelete = ai.AllowDelete;

            await profiles.SaveAiSettings(settings, key is null ? null : Reveal(ai.ApiKey, key), ct);
        }

        foreach (var query in bundled.SavedQueries)
        {
            await profiles.SaveQuery(
                new SavedQuery { ProfileId = profile.Id, Name = query.Name, Sql = query.Sql },
                ct);
        }
    }

    // ── Passphrase crypto ───────────────────────────────────────────────

    /// <summary>
    /// Derives the file key and proves it by decrypting the first secret in the bundle. Returns
    /// false for a wrong passphrase rather than throwing: it is a typo, not an exception.
    /// </summary>
    bool TryUnlock(ConnectionBundle bundle, string passphrase, out byte[]? key)
    {
        key = null;
        if (bundle.Salt is null)
            return false;

        try
        {
            var candidate = DeriveKey(passphrase, Convert.FromBase64String(bundle.Salt));

            var probe = bundle.Connections
                .Select(c => c.ConnectionString ?? c.Password ?? c.Ai?.ApiKey)
                .FirstOrDefault(v => v is not null);

            // AES-GCM authenticates, so a wrong key fails the tag check rather than returning junk -
            // which is what makes this a reliable test rather than a guess.
            if (probe is not null)
                Unprotect(probe, candidate);

            key = candidate;
            return true;
        }
        catch (Exception ex) when (ex is CryptographicException or FormatException)
        {
            return false;
        }
    }

    static byte[] DeriveKey(string passphrase, byte[] salt)
        => Rfc2898DeriveBytes.Pbkdf2(
            Encoding.UTF8.GetBytes(passphrase),
            salt,
            Iterations,
            HashAlgorithmName.SHA256,
            32);

    static string Protect(string? plaintext, byte[] key)
    {
        if (string.IsNullOrEmpty(plaintext))
            return "";

        var nonce = RandomNumberGenerator.GetBytes(AesGcm.NonceByteSizes.MaxSize);
        var plainBytes = Encoding.UTF8.GetBytes(plaintext);
        var cipher = new byte[plainBytes.Length];
        var tag = new byte[AesGcm.TagByteSizes.MaxSize];

        using var aes = new AesGcm(key, tag.Length);
        aes.Encrypt(nonce, plainBytes, cipher, tag);

        return Prefix + Convert.ToBase64String([.. nonce, .. tag, .. cipher]);
    }

    static string Unprotect(string value, byte[] key)
    {
        if (!value.StartsWith(Prefix, StringComparison.Ordinal))
            throw new CryptographicException("That value was not written by this tool's export.");

        var blob = Convert.FromBase64String(value[Prefix.Length..]);
        var nonceSize = AesGcm.NonceByteSizes.MaxSize;
        var tagSize = AesGcm.TagByteSizes.MaxSize;

        var nonce = blob.AsSpan(0, nonceSize);
        var tag = blob.AsSpan(nonceSize, tagSize);
        var cipher = blob.AsSpan(nonceSize + tagSize);
        var plain = new byte[cipher.Length];

        using var aes = new AesGcm(key, tagSize);
        aes.Decrypt(nonce, cipher, tag, plain);

        return Encoding.UTF8.GetString(plain);
    }

    static string Reveal(string? value, byte[] key)
        => string.IsNullOrEmpty(value) ? "" : Unprotect(value, key);
}
