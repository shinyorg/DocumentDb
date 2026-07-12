namespace Shiny.DocumentDb;

/// <summary>Default <see cref="IDocumentTransaction"/> — wraps the store's <see cref="DocumentStore.ExplicitUnit"/>
/// and notifies its owning <see cref="DocumentSession"/> when it closes.</summary>
public sealed class DocumentTransaction : IDocumentTransaction
{
    readonly DocumentSession session;
    readonly DocumentStore.ExplicitUnit unit;
    bool done;

    internal DocumentTransaction(DocumentSession session, DocumentStore.ExplicitUnit unit)
    {
        this.session = session;
        this.unit = unit;
    }

    public bool IsActive => !this.done;

    public async Task Commit(CancellationToken cancellationToken = default)
    {
        if (this.done) return;
        this.done = true;
        await this.unit.CommitAsync(cancellationToken).ConfigureAwait(false);
        this.session.OnTransactionClosed();
    }

    public async Task Rollback(CancellationToken cancellationToken = default)
    {
        if (this.done) return;
        this.done = true;
        await this.unit.RollbackAsync(cancellationToken).ConfigureAwait(false);
        this.session.OnTransactionClosed();
    }

    public async ValueTask DisposeAsync()
    {
        if (this.done) return;
        this.done = true;
        await this.unit.DisposeAsync().ConfigureAwait(false);   // rolls back
        this.session.OnTransactionClosed();
    }
}
