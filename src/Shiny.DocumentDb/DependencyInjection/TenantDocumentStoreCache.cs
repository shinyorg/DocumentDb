using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;

namespace Shiny.DocumentDb;

/// <summary>
/// The tenant-per-database store cache: one <see cref="IDocumentStore"/> per tenant id, built on first
/// resolution, bounded by <see cref="TenantStoreOptions.MaxCachedStores"/> and
/// <see cref="TenantStoreOptions.IdleTimeout"/>, and disposed only once every in-flight request holding it has
/// released its lease. Registered as a singleton by <c>AddMultiTenantDocumentStore</c> and surfaced publicly as
/// <see cref="ITenantStoreManager"/>.
/// </summary>
sealed class TenantDocumentStoreCache : ITenantStoreManager, IDisposable
{
    // Lazy<Task<…>>: Lazy so a cold-start stampede builds exactly one store (a bare GetOrAdd runs its factory
    // on every racing thread and silently drops the losers, undisposed), Task because initialization is async
    // and must finish before the first hand-out.
    readonly ConcurrentDictionary<string, Lazy<Task<TenantStoreEntry>>> entries = new(StringComparer.Ordinal);
    readonly object trimGate = new();
    readonly IServiceProvider services;
    readonly Func<string, IDocumentStore> storeFactory;
    readonly TimeProvider clock;
    bool disposed;

    public TenantDocumentStoreCache(IServiceProvider services, Func<string, IDocumentStore> storeFactory, TenantStoreOptions options)
    {
        this.services = services;
        this.storeFactory = storeFactory;
        this.Options = options;
        this.clock = services.GetService<TimeProvider>() ?? TimeProvider.System;
    }

    public TenantStoreOptions Options { get; }

    public IReadOnlyCollection<string> ActiveTenants => this.entries.Keys.ToArray();

    public async Task WarmAsync(string tenantId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tenantId);
        await this.GetEntryAsync(tenantId, cancellationToken).ConfigureAwait(false);
    }

    public async Task EvictAsync(string tenantId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tenantId);

        if (this.entries.TryRemove(tenantId, out var lazy))
        {
            TenantStoreEntry? entry = null;
            try
            {
                entry = await lazy.Value.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // The store never finished being built — there is nothing to close.
            }

            if (entry != null)
                await entry.EvictAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Takes a lease on the current tenant's store. The lease is what the DI scope owns and disposes; the
    /// store outlives it. Blocks on first-touch creation because DI factories are synchronous — call
    /// <see cref="WarmAsync"/> to move that cost off the request path.
    /// </summary>
    public TenantStoreLease Lease(string tenantId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tenantId);
        ObjectDisposedException.ThrowIf(this.disposed, this);

        while (true)
        {
            var entry = this.GetEntryAsync(tenantId, CancellationToken.None).GetAwaiter().GetResult();
            var lease = entry.TryLease(this.clock.GetUtcNow());

            if (lease != null)
                return lease;

            // The entry was evicted between lookup and lease — drop it and build a fresh one.
            this.entries.TryRemove(new KeyValuePair<string, Lazy<Task<TenantStoreEntry>>>(tenantId, entry.Owner!));
        }
    }

    async Task<TenantStoreEntry> GetEntryAsync(string tenantId, CancellationToken cancellationToken)
    {
        var lazy = this.entries.GetOrAdd(
            tenantId,
            id => new Lazy<Task<TenantStoreEntry>>(() => this.CreateAsync(id), LazyThreadSafetyMode.ExecutionAndPublication)
        );

        try
        {
            var entry = await lazy.Value.WaitAsync(cancellationToken).ConfigureAwait(false);
            entry.Owner = lazy;
            return entry;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // A failed build must not be cached: Lazy remembers the faulted task forever, so the next
            // resolution would replay the same exception instead of retrying.
            this.entries.TryRemove(new KeyValuePair<string, Lazy<Task<TenantStoreEntry>>>(tenantId, lazy));
            throw;
        }
    }

    async Task<TenantStoreEntry> CreateAsync(string tenantId)
    {
        var store = this.storeFactory(tenantId);
        TenantStoreOwnership.Take(store);

        var hook = this.Options.OnTenantStoreCreated;
        if (hook != null)
        {
            try
            {
                // CancellationToken.None deliberately: this task is shared by every caller racing the same
                // cold tenant, so one caller's cancellation must not tear down the others' store.
                await hook(new TenantStoreInitContext(tenantId, store, this.services, CancellationToken.None)).ConfigureAwait(false);
            }
            catch
            {
                TenantStoreOwnership.Dispose(store);
                throw;
            }
        }

        var entry = new TenantStoreEntry(tenantId, store, this.clock.GetUtcNow());
        this.TrimToCapacity();
        return entry;
    }

    // Size-bound the cache. Runs while the new entry's task is still in flight, so the entry being built is
    // never its own victim.
    void TrimToCapacity()
    {
        if (this.Options.MaxCachedStores != int.MaxValue && this.entries.Count > this.Options.MaxCachedStores)
        {
            lock (this.trimGate)
            {
                while (this.entries.Count > this.Options.MaxCachedStores && this.TryEvictLeastRecentlyUsed())
                {
                    // TryEvictLeastRecentlyUsed drops one entry per pass; the loop stops when it runs out.
                }
            }
        }
    }

    bool TryEvictLeastRecentlyUsed()
    {
        var candidates = this.entries
            .ToArray()
            .Where(kvp => kvp.Value.IsValueCreated && kvp.Value.Value.IsCompletedSuccessfully)
            .Select(kvp => (Key: kvp.Key, Lazy: kvp.Value, Entry: kvp.Value.Value.Result))
            .Where(c => !c.Entry.IsEvicted)
            .OrderBy(c => c.Entry.LastUsedUtc)
            .ToArray();

        // Prefer a store nobody is using; if every one of them is leased, evict the least recently used
        // anyway — the lease keeps it alive until that request finishes.
        var victim = candidates.FirstOrDefault(c => c.Entry.LeaseCount == 0);
        if (victim.Entry == null)
            victim = candidates.FirstOrDefault();

        var found = victim.Entry != null;
        if (found && this.entries.TryRemove(new KeyValuePair<string, Lazy<Task<TenantStoreEntry>>>(victim.Key, victim.Lazy)))
            _ = victim.Entry.EvictAsync(CancellationToken.None);

        return found;
    }

    /// <summary>Evicts every store idle for longer than <see cref="TenantStoreOptions.IdleTimeout"/>.</summary>
    internal void SweepIdle()
    {
        if (this.Options.IdleTimeout is { } timeout)
        {
            var cutoff = this.clock.GetUtcNow() - timeout;
            foreach (var kvp in this.entries.ToArray())
            {
                if (kvp.Value.IsValueCreated && kvp.Value.Value.IsCompletedSuccessfully)
                {
                    var entry = kvp.Value.Value.Result;
                    if (entry.LeaseCount == 0 &&
                        entry.LastUsedUtc <= cutoff &&
                        this.entries.TryRemove(new KeyValuePair<string, Lazy<Task<TenantStoreEntry>>>(kvp.Key, kvp.Value)))
                    {
                        _ = entry.EvictAsync(CancellationToken.None);
                    }
                }
            }
        }
    }

    public void Dispose()
    {
        this.disposed = true;
        foreach (var kvp in this.entries.ToArray())
        {
            if (this.entries.TryRemove(new KeyValuePair<string, Lazy<Task<TenantStoreEntry>>>(kvp.Key, kvp.Value)) &&
                kvp.Value.IsValueCreated &&
                kvp.Value.Value.IsCompletedSuccessfully)
            {
                kvp.Value.Value.Result.DisposeNow();
            }
        }
    }
}

/// <summary>One cached tenant store plus the lease accounting that keeps it alive while requests use it.</summary>
sealed class TenantStoreEntry
{
    readonly object gate = new();
    readonly TaskCompletionSource drained = new(TaskCreationOptions.RunContinuationsAsynchronously);
    int leases;
    bool evicted;
    bool storeDisposed;

    public TenantStoreEntry(string tenantId, IDocumentStore store, DateTimeOffset createdUtc)
    {
        this.TenantId = tenantId;
        this.Store = store;
        this.LastUsedUtc = createdUtc;
    }

    public string TenantId { get; }
    public IDocumentStore Store { get; }
    public DateTimeOffset LastUsedUtc { get; private set; }
    public int LeaseCount => Volatile.Read(ref this.leases);

    /// <summary>The dictionary slot this entry came from, so a racing lease can remove the exact one it lost to.</summary>
    public Lazy<Task<TenantStoreEntry>>? Owner { get; set; }

    public bool IsEvicted
    {
        get
        {
            lock (this.gate)
                return this.evicted;
        }
    }

    /// <summary>Takes a lease, or returns null if this entry has already been evicted.</summary>
    public TenantStoreLease? TryLease(DateTimeOffset now)
    {
        lock (this.gate)
        {
            if (this.evicted)
                return null;

            this.leases++;
            this.LastUsedUtc = now;
            return new TenantStoreLease(this);
        }
    }

    internal void Release()
    {
        var dispose = false;
        lock (this.gate)
        {
            this.leases--;
            if (this.leases == 0 && this.evicted && !this.storeDisposed)
            {
                this.storeDisposed = true;
                dispose = true;
            }
        }

        if (dispose)
            this.DisposeStore();
    }

    /// <summary>Marks the entry evicted and disposes the store once the last lease is returned.</summary>
    public Task EvictAsync(CancellationToken cancellationToken)
    {
        var dispose = false;
        lock (this.gate)
        {
            this.evicted = true;
            if (this.leases == 0 && !this.storeDisposed)
            {
                this.storeDisposed = true;
                dispose = true;
            }
        }

        if (dispose)
        {
            this.DisposeStore();
            return Task.CompletedTask;
        }
        return this.drained.Task.WaitAsync(cancellationToken);
    }

    /// <summary>Container shutdown: nothing can still be running, so close the store without waiting.</summary>
    public void DisposeNow()
    {
        var dispose = false;
        lock (this.gate)
        {
            this.evicted = true;
            if (!this.storeDisposed)
            {
                this.storeDisposed = true;
                dispose = true;
            }
        }

        if (dispose)
            this.DisposeStore();
    }

    void DisposeStore()
    {
        TenantStoreOwnership.Dispose(this.Store);
        this.drained.TrySetResult();
    }
}

/// <summary>
/// A scope's claim on a tenant store. Registered as a scoped service so the container disposes it — and
/// therefore releases the claim — when the request ends. Eviction waits for the last one.
/// </summary>
sealed class TenantStoreLease : IDisposable
{
    readonly TenantStoreEntry entry;
    int released;

    public TenantStoreLease(TenantStoreEntry entry) => this.entry = entry;

    public IDocumentStore Store => this.entry.Store;

    public void Dispose()
    {
        if (Interlocked.Exchange(ref this.released, 1) == 0)
            this.entry.Release();
    }
}
