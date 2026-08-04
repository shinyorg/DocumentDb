using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.VectorData;

namespace Shiny.DocumentDb.Extensions.VectorData;

/// <summary>
/// The record types <c>MapVectorRecord&lt;T&gt;</c> has mapped, keyed both by CLR type (what
/// <c>GetCollection&lt;TKey, TRecord&gt;</c> looks up) and by collection name (what
/// <c>ListCollectionNamesAsync</c> / <c>CollectionExistsAsync</c> answer from).
/// <para>
/// Process-wide, like <c>SoftDelete</c>'s: a record's key/vector shape is a property of the record type itself,
/// not of any one store, so the connector can resolve it from a bare <see cref="IDocumentStore"/>. Re-registering
/// the same type under the same name is a no-op; under a different name it throws.
/// </para>
/// </summary>
public static class VectorRecordRegistry
{
    static readonly ConcurrentDictionary<Type, VectorRecordRegistration> byType = new();

    /// <summary>Records a built model so the connector can find it later. Called by <c>MapVectorRecord&lt;T&gt;</c>.</summary>
    public static void Register<TRecord>(VectorRecordModel<TRecord> model) where TRecord : class
    {
        ArgumentNullException.ThrowIfNull(model);

        var registration = new VectorRecordRegistration(
            model.CollectionName,
            typeof(TRecord),
            model,
            (store, ct) => store.Clear<TRecord>(ct)
        );

        var existing = byType.GetOrAdd(typeof(TRecord), registration);
        if (!ReferenceEquals(existing, registration) && existing.CollectionName != registration.CollectionName)
            throw new InvalidOperationException(
                $"'{typeof(TRecord).Name}' is already mapped to the vector collection '{existing.CollectionName}'; it cannot also be mapped to '{registration.CollectionName}'. A record type belongs to one collection.");
    }

    /// <summary>The model for <typeparamref name="TRecord"/>, or a clear explanation of what was not registered.</summary>
    public static VectorRecordModel<TRecord> Require<TRecord>() where TRecord : class
        => byType.TryGetValue(typeof(TRecord), out var registration)
            ? (VectorRecordModel<TRecord>)registration.Model
            : throw new InvalidOperationException(
                $"'{typeof(TRecord).Name}' is not mapped as a vector record. Call options.MapVectorRecord<{typeof(TRecord).Name}>() while configuring the store — DocumentDb fixes its vector mappings when the store is built, so a record cannot be introduced at GetCollection time.");

    /// <summary>Every registered collection name, ordered for a stable <c>ListCollectionNamesAsync</c>.</summary>
    public static IEnumerable<string> CollectionNames
        => byType.Values.Select(x => x.CollectionName).Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal);

    /// <summary>The registration behind a collection name, or null when nothing answers to it.</summary>
    public static VectorRecordRegistration? Find(string collectionName)
        => byType.Values.FirstOrDefault(x => string.Equals(x.CollectionName, collectionName, StringComparison.Ordinal));

    // Test seam: the registry is process-wide, so a test that maps the same record type two different ways needs
    // a way back to a clean slate.
    internal static void Reset() => byType.Clear();
}

/// <summary>One mapped record type: its collection name, its model, and a type-erased <c>Clear</c>.</summary>
/// <param name="CollectionName">The MEVD collection name.</param>
/// <param name="RecordType">The CLR record type.</param>
/// <param name="Model">The <see cref="VectorRecordModel{TRecord}"/> for <paramref name="RecordType"/>.</param>
/// <param name="Clear">Deletes every document of the type — closed over the generic at registration so the
/// store-level <c>EnsureCollectionDeletedAsync(name)</c> needs no reflection.</param>
public sealed record VectorRecordRegistration(
    string CollectionName,
    Type RecordType,
    object Model,
    Func<IDocumentStore, CancellationToken, Task<int>> Clear
);

/// <summary>
/// Maps a <see cref="Microsoft.Extensions.VectorData"/> record type onto a DocumentDb store — the one call that
/// turns MEVD decoration into the store's own vector mapping.
/// </summary>
public static class VectorRecordOptionsExtensions
{
    /// <summary>
    /// Reads <typeparamref name="TRecord"/>'s <c>[VectorStoreKey]</c> / <c>[VectorStoreVector]</c> decoration (or
    /// <paramref name="definition"/>, which wins over it) and registers the matching
    /// <c>MapVectorProperty&lt;TRecord&gt;</c> against <paramref name="options"/>, so the store provisions the
    /// vector column and index at table-init like any other mapped embedding.
    /// <para>
    /// The <c>[VectorStoreKey]</c> property must also be the document's id — name it <c>Id</c> (the convention) or
    /// map it with your options' <c>MapIdProperty&lt;TRecord&gt;</c>, otherwise the by-key reads and writes have
    /// nothing to address.
    /// </para>
    /// <para>
    /// Works on every vector-capable provider through <see cref="IDocumentStoreOptions"/>, so it returns that
    /// interface rather than your concrete options type — call it last in a fluent chain, or as its own statement.
    /// Providers with no vector support throw here rather than at query time.
    /// </para>
    /// </summary>
    /// <param name="options">The store options being configured.</param>
    /// <param name="collectionName">The MEVD collection name. Defaults to the CLR type name.</param>
    /// <param name="definition">An explicit record definition, used instead of the record's attributes.</param>
    public static IDocumentStoreOptions MapVectorRecord<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] TRecord>(
        this IDocumentStoreOptions options,
        string? collectionName = null,
        VectorStoreCollectionDefinition? definition = null) where TRecord : class
    {
        ArgumentNullException.ThrowIfNull(options);

        // Shape first, so a malformed record says what is wrong with the record.
        var model = definition == null
            ? VectorRecordModel.FromAttributes<TRecord>(collectionName)
            : VectorRecordModel.FromDefinition<TRecord>(definition, collectionName);

        // The whole connector is ANN search, so a store that cannot do vectors is refused here rather than at
        // the configuration validation pass — there is nothing usable to build even if the mapping is accepted.
        var caps = options.Capabilities;
        if (!caps.Vector)
            throw new NotSupportedException(
                $"MapVectorRecord<{typeof(TRecord).Name}> needs ANN vector search, which {caps.ProviderName} does not support. " +
                "Point the vector store at one of the relational providers, Cosmos, MongoDB Atlas, or Redis.");

        options.Mappings.MapVectorProperty(
            model.VectorPropertyName,
            model.GetVector,
            model.SetVector,
            model.Dimensions,
            model.Metric,
            model.IndexKind,
            configureIndex: null
        );
        VectorRecordRegistry.Register(model);
        return options;
    }
}
