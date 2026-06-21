namespace Shiny.DocumentDb;

/// <summary>
/// An <see cref="IDocumentSeeder"/> backed by an inline delegate — the lightweight option when a full
/// class isn't warranted. Created by the <c>AddDocumentSeeder(name, version, seed)</c> overload.
/// </summary>
public sealed class DelegateDocumentSeeder : IDocumentSeeder
{
    readonly Func<IDocumentStore, CancellationToken, Task> seed;

    public DelegateDocumentSeeder(string name, int version, Func<IDocumentStore, CancellationToken, Task> seed)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(seed);
        this.Name = name;
        this.Version = version;
        this.seed = seed;
    }

    public string Name { get; }
    public int Version { get; }

    public Task SeedAsync(IDocumentStore store, CancellationToken cancellationToken)
        => this.seed(store, cancellationToken);
}
