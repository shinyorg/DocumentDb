using System.Text.Json;
using ShinyDocDbMyAdmin.Models;
using ShinyDocDbMyAdmin.Providers;
using Shiny.DocumentDb;
using Shiny.DocumentDb.Sqlite;

namespace ShinyDocDbMyAdmin.Services;

/// <summary>
/// Connection profiles and saved queries, kept in the tool's own Shiny.DocumentDb SQLite store.
/// Secrets go in encrypted and only come back out through <see cref="Resolve"/>.
/// </summary>
/// <remarks>
/// Connections handed in by the host (see <see cref="ProvidedConnections"/>) are merged into the same
/// surface so every page can treat them like any other profile - they just cannot be saved or deleted.
/// </remarks>
public sealed class ProfileStore
{
    readonly IDocumentStore store;
    readonly SecretProtector protector;
    readonly AppPaths paths;
    readonly ProvidedConnections provided;
    readonly DemoMode demo;
    readonly AdminJsonContext json;

    public ProfileStore(AppPaths paths, SecretProtector protector, ProvidedConnections provided, DemoMode demo)
    {
        this.paths = paths;
        this.protector = protector;
        this.provided = provided;
        this.demo = demo;
        this.json = new AdminJsonContext(new JsonSerializerOptions(JsonSerializerDefaults.Web));

        var options = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider($"Data Source={paths.ProfileDatabasePath}"),
            JsonSerializerOptions = this.json.Options,
            UseReflectionFallback = false
        };
        options.ConfigureDocument<ConnectionProfile>(cfg => cfg.Table = "connections");
        options.ConfigureDocument<SavedQuery>(cfg => cfg.Table = "saved_queries");
        options.ConfigureDocument<AiConnectionSettings>(cfg => cfg.Table = "ai_settings");

        this.store = new DocumentStore(options);
    }

    // ── Profiles ────────────────────────────────────────────────────────

    public async Task<IReadOnlyList<ConnectionProfile>> List(CancellationToken ct = default)
    {
        var all = await this.store.Query<ConnectionProfile>().ToList(ct);

        // Host-provided connections lead: they describe the app you are actually running.
        return
        [
            .. this.provided.Profiles,
            .. all.OrderBy(x => x.Name, StringComparer.OrdinalIgnoreCase)
        ];
    }

    public Task<ConnectionProfile?> Get(string id, CancellationToken ct = default)
        => this.provided.Find(id) is { } found
            ? Task.FromResult<ConnectionProfile?>(found.Profile)
            : this.store.Get<ConnectionProfile>(id, cancellationToken: ct);

    /// <summary>True when the profile came from the host and so cannot be edited or deleted here.</summary>
    public bool IsProvided(string id) => this.provided.IsProvided(id);

    /// <summary>
    /// Saves a profile whose secret fields are still plaintext. Pass the previous values through
    /// unchanged and they are simply re-encrypted.
    /// </summary>
    public async Task Save(ConnectionProfile profile, string connectionString, string? password, CancellationToken ct = default)
    {
        this.AssertNotProvided(profile.Id);
        profile.ConnectionString = this.protector.Protect(connectionString);
        profile.Password = string.IsNullOrEmpty(password) ? null : this.protector.Protect(password);

        // Same lifecycle as the other two: typed in plaintext, stored wrapped, only unwrapped at the point
        // one is actually used. Entries missing an id or key material are dropped rather than stored empty -
        // a key ring with a blank slot fails at read time with nothing to point at.
        profile.EncryptionKeys =
        [
            .. profile.EncryptionKeys
                .Where(k => !string.IsNullOrWhiteSpace(k.KeyId) && !string.IsNullOrWhiteSpace(k.Key))
                .Select(k => new EncryptionKeyEntry { KeyId = k.KeyId.Trim(), Key = this.protector.Protect(k.Key.Trim()) })
        ];

        var existing = await this.store.Get<ConnectionProfile>(profile.Id, cancellationToken: ct);
        if (existing is null)
            await this.store.Insert(profile, cancellationToken: ct);
        else
            // Full replace rather than Upsert: an Upsert merges, so clearing a password would not stick.
            await this.store.Update(profile, cancellationToken: ct);
    }

    /// <summary>
    /// Replaces the connection's soft-delete declarations, leaving every other field - including the
    /// secrets - untouched. Separate from <see cref="Save"/> because declaring a flag is something an
    /// operator does from the Structure tab, and it must not require re-typing a connection string.
    /// </summary>
    /// <remarks>
    /// Entries missing a type or a path are dropped rather than stored blank: a declaration with no path
    /// would turn every delete of that type into a write to nowhere.
    /// </remarks>
    public async Task SaveSoftDeleteFlags(string profileId, IReadOnlyList<SoftDeleteFlag> flags, CancellationToken ct = default)
    {
        this.AssertNotProvided(profileId);

        var profile = await this.store.Get<ConnectionProfile>(profileId, cancellationToken: ct)
                      ?? throw new InvalidOperationException($"Connection '{profileId}' no longer exists.");

        profile.SoftDeleteFlags =
        [
            .. flags
                .Where(f => !string.IsNullOrWhiteSpace(f.TypeName) && !string.IsNullOrWhiteSpace(f.PropertyPath))
                .GroupBy(f => f.TypeName.Trim(), StringComparer.Ordinal)
                .Select(g => new SoftDeleteFlag
                {
                    // One flag per type, exactly as the library allows - SoftDelete.Register throws on a
                    // second mapping for the same document type.
                    TypeName = g.Key,
                    PropertyPath = g.Last().PropertyPath.Trim(),
                    FlagKind = g.Last().FlagKind
                })
        ];

        await this.store.Update(profile, cancellationToken: ct);
    }

    public async Task Delete(string id, CancellationToken ct = default)
    {
        this.AssertNotProvided(id);
        var profile = await this.store.Get<ConnectionProfile>(id, cancellationToken: ct);
        await this.store.Remove<ConnectionProfile>(id, cancellationToken: ct);

        // An uploaded database belongs to the profile, so it goes with it. A referenced one never does.
        if (profile?.UploadedFileName is not null)
        {
            var dir = this.paths.UploadDirectoryFor(id);
            if (Directory.Exists(dir))
                Directory.Delete(dir, recursive: true);
        }
    }

    public async Task MarkOpened(string id, CancellationToken ct = default)
    {
        // Nothing to record for a provided connection: it is not stored, so there is nowhere to put it.
        if (this.provided.IsProvided(id))
            return;

        var profile = await this.store.Get<ConnectionProfile>(id, cancellationToken: ct);
        if (profile is null)
            return;

        profile.LastOpenedAt = DateTimeOffset.UtcNow;
        await this.store.Update(profile, cancellationToken: ct);
    }

    /// <summary>Decrypts a profile into the form the provider factory needs.</summary>
    public ResolvedProfile Resolve(ConnectionProfile profile)
    {
        var descriptor = ProviderCatalog.Get(profile.Provider);

        // A provided connection never went through the protector - its secrets came in from the host
        // and only ever live in memory.
        var supplied = this.provided.Find(profile.Id);
        var connectionString = supplied?.ConnectionString ?? this.protector.Unprotect(profile.ConnectionString);
        var password = supplied is not null
            ? supplied.Password
            : profile.Password is null ? null : this.protector.Unprotect(profile.Password);

        string? filePath = null;
        if (descriptor.IsFileBased)
        {
            filePath = SqliteConnectionStrings.ExtractDataSource(connectionString);
            connectionString = SqliteConnectionStrings.EnsureDataSource(connectionString);
        }

        return new ResolvedProfile(profile, connectionString, password, filePath);
    }

    public async Task<ResolvedProfile?> ResolveById(string id, CancellationToken ct = default)
    {
        var profile = await this.Get(id, ct);
        return profile is null ? null : this.Resolve(profile);
    }

    /// <summary>The plaintext connection string, for pre-filling the edit form.</summary>
    public string RevealConnectionString(ConnectionProfile profile)
        => this.provided.Find(profile.Id)?.ConnectionString ?? this.protector.Unprotect(profile.ConnectionString);

    public string? RevealPassword(ConnectionProfile profile)
        => this.provided.Find(profile.Id) is { } supplied
            ? supplied.Password
            : profile.Password is null ? null : this.protector.Unprotect(profile.Password);

    /// <summary>
    /// The plaintext read-only key ring, for building a decryptor or pre-filling the edit form.
    /// </summary>
    /// <remarks>
    /// Host-provided connections carry none: their secrets come in from the host and there is nowhere for
    /// an operator to have typed a data key. Nothing about that is a limitation worth working around - a
    /// host that wants the tool to read protected values can hand it a connection it owns.
    /// </remarks>
    public IReadOnlyList<EncryptionKeyEntry> RevealEncryptionKeys(ConnectionProfile profile)
        => this.provided.IsProvided(profile.Id)
            ? []
            : [.. profile.EncryptionKeys.Select(k => new EncryptionKeyEntry
            {
                KeyId = k.KeyId,
                Key = this.protector.Unprotect(k.Key)
            })];

    void AssertNotProvided(string id)
    {
        // A demo instance publishes a fixed list, and this is the layer every write path already
        // passes through - so the guard sits here rather than being repeated at each caller.
        this.demo.AssertCanManageConnections();

        if (this.provided.IsProvided(id))
            throw new InvalidOperationException(
                "This connection comes from the host environment (an Aspire AppHost, or configuration). " +
                "Change it where it is declared - it cannot be edited or removed from here.");
    }

    // ── Assistant settings ──────────────────────────────────────────────

    /// <summary>
    /// The assistant configuration for a connection, or null when it has never been configured.
    /// <see cref="AiConnectionSettings.ApiKey"/> comes back as ciphertext - use
    /// <see cref="RevealApiKey"/> at the point a client is built.
    /// </summary>
    /// <remarks>
    /// Deliberately not blocked for host-provided connections. Their <i>profile</i> is owned by the
    /// host and cannot be written here, but the assistant configuration is this tool's own and has
    /// nowhere else to live.
    /// </remarks>
    public async Task<AiConnectionSettings?> GetAiSettings(string profileId, CancellationToken ct = default)
    {
        var all = await this.store.Query<AiConnectionSettings>().Where(x => x.ProfileId == profileId).ToList(ct);
        return all.FirstOrDefault();
    }

    /// <summary>Saves settings whose API key is still plaintext; pass the previous value through to keep it.</summary>
    public async Task SaveAiSettings(AiConnectionSettings settings, string? apiKey, CancellationToken ct = default)
    {
        settings.ApiKey = string.IsNullOrWhiteSpace(apiKey) ? null : this.protector.Protect(apiKey);
        settings.UpdatedAt = DateTimeOffset.UtcNow;

        var existing = await this.store.Get<AiConnectionSettings>(settings.Id, cancellationToken: ct);
        if (existing is null)
            await this.store.Insert(settings, cancellationToken: ct);
        else
            // Full replace, not Upsert: an Upsert merges, so clearing the key would not stick - the
            // same reason profile saves replace.
            await this.store.Update(settings, cancellationToken: ct);
    }

    public async Task DeleteAiSettings(string profileId, CancellationToken ct = default)
    {
        if (await this.GetAiSettings(profileId, ct) is { } settings)
            await this.store.Remove<AiConnectionSettings>(settings.Id, cancellationToken: ct);
    }

    /// <summary>The plaintext API key, for building a client or pre-filling the edit form.</summary>
    public string? RevealApiKey(AiConnectionSettings settings)
        => settings.ApiKey is null ? null : this.protector.Unprotect(settings.ApiKey);

    /// <summary>
    /// Encrypts a typed key into an unsaved settings instance, so the "test this configuration"
    /// path can build a client from exactly the same shape a saved one produces rather than needing
    /// a second, plaintext-carrying route into <see cref="AiClientFactory"/>.
    /// </summary>
    public void ProtectInto(AiConnectionSettings settings, string? apiKey)
        => settings.ApiKey = string.IsNullOrWhiteSpace(apiKey) ? null : this.protector.Protect(apiKey);

    // ── Saved queries ───────────────────────────────────────────────────

    public async Task<IReadOnlyList<SavedQuery>> ListQueries(string profileId, CancellationToken ct = default)
    {
        var all = await this.store.Query<SavedQuery>().Where(x => x.ProfileId == profileId).ToList(ct);
        return [.. all.OrderByDescending(x => x.SavedAt)];
    }

    public Task SaveQuery(SavedQuery query, CancellationToken ct = default)
        => this.store.Upsert(query, cancellationToken: ct);

    public Task DeleteQuery(string id, CancellationToken ct = default)
        => this.store.Remove<SavedQuery>(id, cancellationToken: ct);
}
