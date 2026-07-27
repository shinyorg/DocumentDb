using System.Data.Common;
using ShinyDocDbMyAdmin.Models;
using ShinyDocDbMyAdmin.Providers;
using Shiny.DocumentDb;

namespace ShinyDocDbMyAdmin.Services;

/// <summary>
/// A live handle on one target database. Every operation opens and disposes its own connection -
/// the embedded engines (SQLite, DuckDB) lock the file while a connection is open, and an admin
/// tool has no business holding that lock between clicks.
/// </summary>
public sealed class AdminConnection(ResolvedProfile profile, IDatabaseProvider provider) : IDisposable
{
    readonly SemaphoreSlim gate = new(1, 1);

    public ResolvedProfile Profile { get; } = profile;
    public IDatabaseProvider Provider { get; } = provider;
    public ProviderDescriptor Descriptor { get; } = profile.Descriptor;

    public async Task<T> Execute<T>(Func<DbConnection, CancellationToken, Task<T>> work, CancellationToken ct = default)
    {
        await this.gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            await using var connection = this.Provider.CreateConnection();
            await connection.OpenAsync(ct).ConfigureAwait(false);
            await this.Provider.InitializeConnectionAsync(connection, ct).ConfigureAwait(false);
            return await work(connection, ct).ConfigureAwait(false);
        }
        finally
        {
            this.gate.Release();
        }
    }

    public Task Execute(Func<DbConnection, CancellationToken, Task> work, CancellationToken ct = default)
        => this.Execute<object?>(async (c, t) => { await work(c, t); return null; }, ct);

    /// <summary>Throws when the profile is marked read-only. Called at the top of every write path.</summary>
    public void AssertWritable()
    {
        if (this.Profile.ReadOnly)
            throw new InvalidOperationException($"Connection '{this.Profile.Name}' is marked read-only.");
    }

    public void Dispose() => this.gate.Dispose();
}

/// <summary>Caches one <see cref="AdminConnection"/> per profile and rebuilds it when the profile changes.</summary>
public sealed class ConnectionManager(ProfileStore profiles, ILogger<ConnectionManager> logger) : IDisposable
{
    readonly Dictionary<string, Entry> cache = new();
    readonly SemaphoreSlim gate = new(1, 1);

    sealed record Entry(AdminConnection Connection, string Fingerprint);

    public async Task<AdminConnection> Open(string profileId, CancellationToken ct = default)
    {
        var resolved = await profiles.ResolveById(profileId, ct)
                       ?? throw new InvalidOperationException($"Connection '{profileId}' no longer exists.");

        // Re-created whenever any input to the provider changes, so an edited profile takes effect at once.
        var fingerprint = $"{resolved.Provider}|{resolved.ConnectionString}|{resolved.Password}";

        await this.gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (this.cache.TryGetValue(profileId, out var entry))
            {
                if (entry.Fingerprint == fingerprint)
                    return entry.Connection;

                entry.Connection.Dispose();
                this.cache.Remove(profileId);
            }

            var provider = ProviderCatalog.Create(resolved.Provider, resolved.ConnectionString, resolved.Password, resolved.FilePath);
            var connection = new AdminConnection(resolved, provider);
            this.cache[profileId] = new Entry(connection, fingerprint);
            logger.LogInformation("Opened {Provider} connection '{Name}'", resolved.Provider, resolved.Name);
            return connection;
        }
        finally
        {
            this.gate.Release();
        }
    }

    public void Evict(string profileId)
    {
        this.gate.Wait();
        try
        {
            if (this.cache.Remove(profileId, out var entry))
                entry.Connection.Dispose();
        }
        finally
        {
            this.gate.Release();
        }
    }

    public void Dispose()
    {
        foreach (var entry in this.cache.Values)
            entry.Connection.Dispose();

        this.cache.Clear();
        this.gate.Dispose();
    }
}
