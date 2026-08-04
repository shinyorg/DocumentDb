using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.VectorData;

namespace Shiny.DocumentDb.Extensions.VectorData;

/// <summary>
/// A <see cref="VectorStore"/> over any vector-capable <see cref="IDocumentStore"/> — the
/// <c>Microsoft.Extensions.VectorData</c> face of Shiny.DocumentDb, so the same MEVD record model and the same
/// calling code run over SQLite on a device, PostgreSQL/pgvector or SQL Server in production, or Cosmos / Atlas /
/// Redis, swapped by configuration rather than by rewriting against another connector.
/// <para>
/// The connector is <b>thin over a store you already built</b>: DocumentDb fixes its vector mappings when the
/// store is constructed, so every record type must be registered with <c>MapVectorRecord&lt;T&gt;</c> while the
/// options are being configured. <c>GetCollection</c> then only builds a facade — it never opens a second
/// connection pool and never re-provisions a table.
/// </para>
/// </summary>
public sealed class DocumentDbVectorStore : VectorStore
{
    /// <summary>The <c>VectorStoreSystemName</c> this connector reports in metadata and exceptions.</summary>
    public const string SystemName = "shiny.documentdb";

    readonly IDocumentStore store;
    readonly IEmbeddingGenerator? embeddingGenerator;
    readonly VectorStoreMetadata metadata;

    /// <summary>
    /// Wraps a store that has already had its record types mapped with <c>MapVectorRecord&lt;T&gt;</c>.
    /// </summary>
    /// <param name="store">The document store to serve MEVD from.</param>
    /// <param name="embeddingGenerator">
    /// Turns a non-vector search value (a <c>string</c>) into an embedding. Optional — a record's own
    /// <see cref="VectorStoreCollectionDefinition.EmbeddingGenerator"/> wins over it, and searching by a vector
    /// needs neither.
    /// </param>
    /// <exception cref="NotSupportedException">The store's provider has no vector search.</exception>
    public DocumentDbVectorStore(IDocumentStore store, IEmbeddingGenerator? embeddingGenerator = null)
    {
        ArgumentNullException.ThrowIfNull(store);
        if (!store.SupportsVector)
            throw new NotSupportedException(
                $"'{store.GetType().Name}' does not support vector search, so it cannot back a VectorStore. Use a vector-capable provider (SQLite with the vector extension, PostgreSQL/pgvector, CockroachDB, SQL Server, Oracle, DuckDB, CosmosDB, MongoDB Atlas, Amazon DocumentDB, or Redis).");

        this.store = store;
        this.embeddingGenerator = embeddingGenerator;
        this.metadata = new VectorStoreMetadata { VectorStoreSystemName = SystemName };
    }

    /// <inheritdoc/>
    // The base is annotated because most connectors reflect over TRecord here. This one does not: the record's
    // accessors were built (under a DynamicallyAccessedMembers annotation) by MapVectorRecord<T> while the store
    // was being configured, and GetCollection is a dictionary lookup over that registration. Nothing on this path
    // reflects or emits, so the base's warnings do not apply to it.
    [UnconditionalSuppressMessage("Trimming", "IL2046", Justification = "Resolves a pre-built registration; no reflection over TRecord.")]
    [UnconditionalSuppressMessage("AOT", "IL3051", Justification = "Resolves a pre-built registration; no runtime code generation.")]
    public override VectorStoreCollection<TKey, TRecord> GetCollection<TKey, TRecord>(string name, VectorStoreCollectionDefinition? definition = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        var model = VectorRecordRegistry.Require<TRecord>();
        if (!string.Equals(model.CollectionName, name, StringComparison.Ordinal))
            throw new InvalidOperationException(
                $"'{typeof(TRecord).Name}' is mapped to the vector collection '{model.CollectionName}', not '{name}'. Pass the name to MapVectorRecord<{typeof(TRecord).Name}>(\"{name}\") when configuring the store.");

        // A definition handed in here cannot change the store's vector mapping — that was fixed when the store was
        // built — so rather than silently ignoring it, take the one part that is genuinely a runtime concern (the
        // embedding generator) and reject any structural divergence. Supply the definition to MapVectorRecord
        // instead when you want it to drive the mapping.
        if (definition != null)
            RequireConsistent<TRecord>(model, definition, name);

        return new DocumentDbCollection<TKey, TRecord>(
            this.store,
            model,
            definition?.EmbeddingGenerator ?? model.EmbeddingGenerator ?? this.embeddingGenerator
        );
    }

    /// <inheritdoc/>
    /// <exception cref="NotSupportedException">Always — dynamic collections are not supported yet.</exception>
    public override VectorStoreCollection<object, Dictionary<string, object?>> GetDynamicCollection(string name, VectorStoreCollectionDefinition definition)
        => throw new NotSupportedException(
            "Dynamic (Dictionary<string, object?>) collections are not supported by the DocumentDb connector. Use GetCollection<TKey, TRecord> with a mapped record type.");

    /// <inheritdoc/>
    public override async IAsyncEnumerable<string> ListCollectionNamesAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        foreach (var collectionName in VectorRecordRegistry.CollectionNames)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return collectionName;
        }
        await Task.CompletedTask.ConfigureAwait(false);
    }

    /// <inheritdoc/>
    /// <remarks>
    /// True for every mapped collection: DocumentDb provisions a mapped type's table and vector index at
    /// initialization, so a registered record always has storage behind it.
    /// </remarks>
    public override Task<bool> CollectionExistsAsync(string name, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        return Task.FromResult(VectorRecordRegistry.Find(name) != null);
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Clears the mapped type's documents rather than dropping the table — DocumentDb re-provisions a dropped
    /// table only at store construction, so dropping it would leave the store pointing at nothing. Deleting an
    /// unmapped collection succeeds silently, as the contract requires.
    /// </remarks>
    public override async Task EnsureCollectionDeletedAsync(string name, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        var registration = VectorRecordRegistry.Find(name);
        if (registration != null)
            await registration.Clear(this.store, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public override object? GetService(Type serviceType, object? serviceKey = null)
    {
        ArgumentNullException.ThrowIfNull(serviceType);
        if (serviceKey != null)
            return null;

        if (serviceType == typeof(VectorStoreMetadata))
            return this.metadata;

        if (serviceType == typeof(IDocumentStore))
            return this.store;

        return serviceType.IsInstanceOfType(this) ? this : null;
    }

    static void RequireConsistent<TRecord>(VectorRecordModel<TRecord> model, VectorStoreCollectionDefinition definition, string name) where TRecord : class
    {
        var vector = definition.Properties.OfType<VectorStoreVectorProperty>().FirstOrDefault();
        if (vector == null)
            return;

        if (!string.Equals(vector.Name, model.VectorPropertyName, StringComparison.Ordinal) || vector.Dimensions != model.Dimensions)
            throw new InvalidOperationException(
                $"The definition passed to GetCollection(\"{name}\") describes '{vector.Name}' ({vector.Dimensions} dimensions), but '{typeof(TRecord).Name}' is mapped on '{model.VectorPropertyName}' ({model.Dimensions} dimensions). DocumentDb fixes vector mappings when the store is built — pass the definition to MapVectorRecord<{typeof(TRecord).Name}>() instead.");
    }
}
