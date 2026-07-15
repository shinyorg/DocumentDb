namespace Shiny.DocumentDb;

/// <summary>
/// An <see cref="IDocumentMigration"/> backed by inline delegates — the lightweight option when a full
/// class isn't warranted. Created by the <c>AddDocumentMigration(version, name, up, down)</c> overload.
/// A null <c>down</c> leaves the migration non-reversible (its <see cref="Down"/> throws).
/// </summary>
public sealed class DelegateDocumentMigration : IDocumentMigration
{
    readonly Func<IDocumentStore, CancellationToken, Task> up;
    readonly Func<IDocumentStore, CancellationToken, Task>? down;

    public DelegateDocumentMigration(
        long version,
        string name,
        Func<IDocumentStore, CancellationToken, Task> up,
        Func<IDocumentStore, CancellationToken, Task>? down = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(up);
        this.Version = version;
        this.Name = name;
        this.up = up;
        this.down = down;
    }

    public long Version { get; }
    public string Name { get; }

    public Task Up(IDocumentStore store, CancellationToken cancellationToken)
        => this.up(store, cancellationToken);

    public Task Down(IDocumentStore store, CancellationToken cancellationToken)
        => this.down?.Invoke(store, cancellationToken)
            ?? throw new NotSupportedException($"Migration '{this.Name}' ({this.Version}) does not support rollback (no Down delegate).");
}
