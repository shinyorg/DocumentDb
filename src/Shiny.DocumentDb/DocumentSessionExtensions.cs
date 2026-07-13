namespace Shiny.DocumentDb;

/// <summary>Convenience helpers over <see cref="IDocumentSession"/>.</summary>
public static class DocumentSessionExtensions
{
    /// <summary>
    /// Runs <paramref name="worker"/> inside a single explicit transaction: opens one with
    /// <see cref="IDocumentSession.BeginTransaction(CancellationToken)"/>, awaits the worker, optionally flushes
    /// buffered writes with <c>SaveChanges</c> (<paramref name="autoSaveChanges"/>), then commits. If the worker
    /// or the flush throws, the transaction is rolled back (nothing is committed) and the exception propagates.
    /// </summary>
    /// <remarks>
    /// <c>SaveChanges</c> here flushes <b>into</b> this transaction without committing it — a session's
    /// <c>SaveChanges</c> only commits a transaction it opened itself, never one opened by
    /// <c>BeginTransaction</c>/this helper — so the commit is done exactly once, by this method. Explicit
    /// transactions are relational-provider only (others throw from <c>BeginTransaction</c>).
    /// </remarks>
    /// <param name="worker">The work to run inside the transaction (reads, buffered writes, set-based writes).</param>
    /// <param name="autoSaveChanges">When true (default), flush the session's buffered writes before committing.</param>
    public static async Task RunInTransaction(
        this IDocumentSession session,
        Func<Task> worker,
        bool autoSaveChanges = true,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(session);
        ArgumentNullException.ThrowIfNull(worker);

        await using var tx = await session.BeginTransaction(cancellationToken).ConfigureAwait(false);
        await worker().ConfigureAwait(false);
        if (autoSaveChanges)
            await session.SaveChanges(cancellationToken).ConfigureAwait(false);
        await tx.Commit(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// As <see cref="RunInTransaction(IDocumentSession, Func{Task}, bool, CancellationToken)"/>, opening the
    /// transaction at <paramref name="isolationLevel"/> — e.g. <see cref="System.Data.IsolationLevel.Snapshot"/>
    /// for a consistent-read transaction.
    /// </summary>
    public static async Task RunInTransaction(
        this IDocumentSession session,
        System.Data.IsolationLevel isolationLevel,
        Func<Task> worker,
        bool autoSaveChanges = true,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(session);
        ArgumentNullException.ThrowIfNull(worker);

        await using var tx = await session.BeginTransaction(isolationLevel, cancellationToken).ConfigureAwait(false);
        await worker().ConfigureAwait(false);
        if (autoSaveChanges)
            await session.SaveChanges(cancellationToken).ConfigureAwait(false);
        await tx.Commit(cancellationToken).ConfigureAwait(false);
    }

    // ── Open-a-session convenience: provisions a session, hands it to the worker, runs the whole thing in one
    //    transaction, and disposes the session. The worker receives the session (the "document context") so it
    //    doesn't have to capture one. Builds on the IDocumentSession.RunInTransaction above. ────────────────────

    /// <summary>
    /// Opens a session from the factory, runs <paramref name="worker"/> (which receives that
    /// <see cref="IDocumentSession"/>) inside a single transaction, then disposes the session. A one-liner for
    /// "provision a session, do transactional work, clean up" — the worker just does its writes/reads on the
    /// session it's handed.
    /// </summary>
    public static async Task RunInTransaction(
        this IDocumentSessionFactory factory,
        Func<IDocumentSession, Task> worker,
        bool autoSaveChanges = true,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(factory);
        ArgumentNullException.ThrowIfNull(worker);

        await using var session = factory.OpenSession();
        await session.RunInTransaction(() => worker(session), autoSaveChanges, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>As above, on a named store (<see cref="IDocumentSessionFactory.OpenSession(string)"/>).</summary>
    public static async Task RunInTransaction(
        this IDocumentSessionFactory factory,
        string storeName,
        Func<IDocumentSession, Task> worker,
        bool autoSaveChanges = true,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(factory);
        ArgumentNullException.ThrowIfNull(worker);

        await using var session = factory.OpenSession(storeName);
        await session.RunInTransaction(() => worker(session), autoSaveChanges, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Opens a session over the (untyped) store, runs <paramref name="worker"/> (which receives that
    /// <see cref="IDocumentSession"/>) inside a single transaction, then disposes the session. The scope-less
    /// equivalent of the factory overload — handy when you already hold an <see cref="IDocumentStore"/>.
    /// </summary>
    public static async Task RunInTransaction(
        this IDocumentStore store,
        Func<IDocumentSession, Task> worker,
        bool autoSaveChanges = true,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(worker);

        await using var session = store.OpenSession();
        await session.RunInTransaction(() => worker(session), autoSaveChanges, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>As the store overload, opening the transaction at <paramref name="isolationLevel"/>.</summary>
    public static async Task RunInTransaction(
        this IDocumentStore store,
        System.Data.IsolationLevel isolationLevel,
        Func<IDocumentSession, Task> worker,
        bool autoSaveChanges = true,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(worker);

        await using var session = store.OpenSession();
        await session.RunInTransaction(isolationLevel, () => worker(session), autoSaveChanges, cancellationToken).ConfigureAwait(false);
    }
}
