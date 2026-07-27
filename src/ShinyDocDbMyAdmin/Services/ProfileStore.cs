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
    readonly AdminJsonContext json;

    public ProfileStore(AppPaths paths, SecretProtector protector, ProvidedConnections provided)
    {
        this.paths = paths;
        this.protector = protector;
        this.provided = provided;
        this.json = new AdminJsonContext(new JsonSerializerOptions(JsonSerializerDefaults.Web));

        this.store = new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider($"Data Source={paths.ProfileDatabasePath}"),
            JsonSerializerOptions = this.json.Options,
            UseReflectionFallback = false
        }
        .MapTypeToTable<ConnectionProfile>("connections")
        .MapTypeToTable<SavedQuery>("saved_queries"));
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

        var existing = await this.store.Get<ConnectionProfile>(profile.Id, cancellationToken: ct);
        if (existing is null)
            await this.store.Insert(profile, cancellationToken: ct);
        else
            // Full replace rather than Upsert: an Upsert merges, so clearing a password would not stick.
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

    void AssertNotProvided(string id)
    {
        if (this.provided.IsProvided(id))
            throw new InvalidOperationException(
                "This connection comes from the host environment (an Aspire AppHost, or configuration). " +
                "Change it where it is declared - it cannot be edited or removed from here.");
    }

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
