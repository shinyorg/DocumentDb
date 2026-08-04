using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.VectorData;

namespace Shiny.DocumentDb.Extensions.VectorData;

/// <summary>
/// The DocumentDb view of one MEVD record type: which property is the key, which is the embedding, and the
/// vector metadata (<see cref="Dimensions"/> / <see cref="Metric"/> / <see cref="IndexKind"/>) already
/// translated from MEVD's string constants into the store's enums.
/// <para>
/// Built once by <c>MapVectorRecord&lt;T&gt;</c> while the store's options are being configured — which is the
/// only moment DocumentDb can accept a vector mapping — and cached for the connector to read at
/// <c>GetCollection</c> time. See <see cref="VectorRecordRegistry"/>.
/// </para>
/// </summary>
public sealed class VectorRecordModel<TRecord> where TRecord : class
{
    /// <summary>The MEVD collection name this record answers to. Defaults to the CLR type name.</summary>
    public required string CollectionName { get; init; }

    /// <summary>The CLR property carrying <c>[VectorStoreKey]</c>. Must also be the document's id property.</summary>
    public required string KeyPropertyName { get; init; }

    /// <summary>The key property's CLR type — one of <c>string</c>, <see cref="Guid"/>, <c>int</c>, <c>long</c>.</summary>
    public required Type KeyType { get; init; }

    /// <summary>Reads the key off a record (for diagnostics and the by-key write paths).</summary>
    public required Func<TRecord, object?> GetKey { get; init; }

    /// <summary>The CLR property carrying <c>[VectorStoreVector]</c>.</summary>
    public required string VectorPropertyName { get; init; }

    /// <summary>Embedding dimensions, from <c>[VectorStoreVector(dimensions)]</c>.</summary>
    public required int Dimensions { get; init; }

    /// <summary>The distance metric, translated from MEVD's <see cref="DistanceFunction"/> constants.</summary>
    public required VectorDistance Metric { get; init; }

    /// <summary>
    /// The ANN index kind, translated from MEVD's <see cref="IndexKind"/> constants. <c>null</c> when the record
    /// did not ask for one (or asked for <see cref="IndexKind.Dynamic"/>), meaning "take the provider's default".
    /// </summary>
    public VectorIndexKind? IndexKind { get; init; }

    /// <summary>Reads the embedding off a record.</summary>
    public required Func<TRecord, ReadOnlyMemory<float>> GetVector { get; init; }

    /// <summary>Writes the embedding onto a record — used to strip vectors when the caller did not ask for them.</summary>
    public required Action<TRecord, ReadOnlyMemory<float>> SetVector { get; init; }

    /// <summary>
    /// The generator that turns a non-vector search value (a <c>string</c>) into an embedding, when the record's
    /// definition supplied one. Falls back to the store-level generator otherwise.
    /// </summary>
    public IEmbeddingGenerator? EmbeddingGenerator { get; init; }
}

/// <summary>
/// Reads a MEVD record type — its <see cref="VectorStoreKeyAttribute"/> / <see cref="VectorStoreVectorAttribute"/>
/// decoration, or an explicit <see cref="VectorStoreCollectionDefinition"/> — into a
/// <see cref="VectorRecordModel{TRecord}"/> the DocumentDb store can be configured from.
/// </summary>
public static class VectorRecordModel
{
    /// <summary>The key CLR types DocumentDb can store an id as.</summary>
    static readonly Type[] SupportedKeyTypes = [typeof(string), typeof(Guid), typeof(int), typeof(long)];

    /// <summary>
    /// Builds the model from the record's MEVD attributes. Exactly one <c>[VectorStoreKey]</c> and exactly one
    /// <c>[VectorStoreVector]</c> property are required.
    /// </summary>
    public static VectorRecordModel<TRecord> FromAttributes<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] TRecord>(
        string? collectionName = null) where TRecord : class
    {
        var (key, vector) = ReadProperties<TRecord>();
        var attribute = vector.GetCustomAttribute<VectorStoreVectorAttribute>()!;

        RequireNoStorageName<TRecord>(
            typeof(TRecord).GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Select(p => (p.Name, Storage:
                    p.GetCustomAttribute<VectorStoreKeyAttribute>()?.StorageName
                    ?? p.GetCustomAttribute<VectorStoreDataAttribute>()?.StorageName
                    ?? p.GetCustomAttribute<VectorStoreVectorAttribute>()?.StorageName)));

        return Build<TRecord>(
            collectionName,
            key,
            vector,
            attribute.Dimensions,
            attribute.DistanceFunction,
            attribute.IndexKind,
            embeddingGenerator: null
        );
    }

    /// <summary>
    /// Builds the model from an explicit <see cref="VectorStoreCollectionDefinition"/>, which wins over any
    /// attributes on the record (MEVD connector requirement §3). The definition's property <c>Name</c>s are CLR
    /// property names; the accessors are still read off the type.
    /// </summary>
    public static VectorRecordModel<TRecord> FromDefinition<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] TRecord>(
        VectorStoreCollectionDefinition definition,
        string? collectionName = null) where TRecord : class
    {
        ArgumentNullException.ThrowIfNull(definition);

        var keys = definition.Properties.OfType<VectorStoreKeyProperty>().ToList();
        var vectors = definition.Properties.OfType<VectorStoreVectorProperty>().ToList();

        if (keys.Count != 1)
            throw new ArgumentException(
                $"A VectorStoreCollectionDefinition for '{typeof(TRecord).Name}' must declare exactly one key property; found {keys.Count}.",
                nameof(definition));

        if (vectors.Count != 1)
            throw new ArgumentException(
                vectors.Count == 0
                    ? $"A VectorStoreCollectionDefinition for '{typeof(TRecord).Name}' must declare a vector property."
                    : $"'{typeof(TRecord).Name}' declares {vectors.Count} vector properties. DocumentDb maps one embedding per document type, so multi-vector records are not supported.",
                nameof(definition));

        var vectorProperty = vectors[0];

        RequireNoStorageName<TRecord>(definition.Properties.Select(p => (p.Name, Storage: p.StorageName)));

        return Build<TRecord>(
            collectionName,
            RequireProperty<TRecord>(keys[0].Name, "key"),
            RequireProperty<TRecord>(vectorProperty.Name, "vector"),
            vectorProperty.Dimensions,
            vectorProperty.DistanceFunction,
            vectorProperty.IndexKind,
            vectorProperty.EmbeddingGenerator ?? definition.EmbeddingGenerator
        );
    }

    static VectorRecordModel<TRecord> Build<TRecord>(
        string? collectionName,
        PropertyInfo key,
        PropertyInfo vector,
        int dimensions,
        string? distanceFunction,
        string? indexKind,
        IEmbeddingGenerator? embeddingGenerator) where TRecord : class
    {
        if (dimensions <= 0)
            throw new ArgumentException(
                $"'{typeof(TRecord).Name}.{vector.Name}' must declare a positive vector dimension count.",
                nameof(dimensions));

        if (vector.PropertyType != typeof(ReadOnlyMemory<float>))
            throw new NotSupportedException(
                $"'{typeof(TRecord).Name}.{vector.Name}' is a {vector.PropertyType.Name}. DocumentDb stores embeddings as ReadOnlyMemory<float>, which is the only vector property type this connector maps.");

        if (!SupportedKeyTypes.Contains(key.PropertyType))
            throw new NotSupportedException(
                $"'{typeof(TRecord).Name}.{key.Name}' is a {key.PropertyType.Name}. A DocumentDb key must be a string, Guid, int, or long.");

        if (vector.GetSetMethod() == null)
            throw new NotSupportedException(
                $"'{typeof(TRecord).Name}.{vector.Name}' has no public setter. The connector writes the embedding back when a search is asked not to include vectors.");

        return new VectorRecordModel<TRecord>
        {
            CollectionName = collectionName ?? typeof(TRecord).Name,
            KeyPropertyName = key.Name,
            KeyType = key.PropertyType,
            GetKey = record => key.GetValue(record),
            VectorPropertyName = vector.Name,
            Dimensions = dimensions,
            Metric = MapDistance(distanceFunction),
            IndexKind = MapIndexKind(indexKind),
            GetVector = record => (ReadOnlyMemory<float>)vector.GetValue(record)!,
            SetVector = (record, value) => vector.SetValue(record, value),
            EmbeddingGenerator = embeddingGenerator
        };
    }

    static (PropertyInfo Key, PropertyInfo Vector) ReadProperties<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] TRecord>()
    {
        var properties = typeof(TRecord).GetProperties(BindingFlags.Public | BindingFlags.Instance);
        var keys = properties.Where(p => p.GetCustomAttribute<VectorStoreKeyAttribute>() != null).ToList();
        var vectors = properties.Where(p => p.GetCustomAttribute<VectorStoreVectorAttribute>() != null).ToList();

        if (keys.Count != 1)
            throw new InvalidOperationException(
                keys.Count == 0
                    ? $"'{typeof(TRecord).Name}' has no [VectorStoreKey] property. Decorate the id property, or supply a VectorStoreCollectionDefinition."
                    : $"'{typeof(TRecord).Name}' has {keys.Count} [VectorStoreKey] properties; a document has exactly one id.");

        if (vectors.Count != 1)
            throw new InvalidOperationException(
                vectors.Count == 0
                    ? $"'{typeof(TRecord).Name}' has no [VectorStoreVector] property. Decorate the embedding property, or supply a VectorStoreCollectionDefinition."
                    : $"'{typeof(TRecord).Name}' has {vectors.Count} [VectorStoreVector] properties. DocumentDb maps one embedding per document type, so multi-vector records are not supported.");

        return (keys[0], vectors[0]);
    }

    /// <summary>
    /// DocumentDb stores documents as System.Text.Json, so a property's storage name <i>is</i> whatever the
    /// store's <c>JsonSerializerOptions</c> produce — naming policy plus <c>[JsonPropertyName]</c>. Honouring
    /// MEVD's <c>StorageName</c> on top of that would give one property two different names depending on which
    /// API wrote it, so it is rejected rather than silently ignored: a filter over a renamed property would
    /// otherwise match nothing and report no error.
    /// </summary>
    static void RequireNoStorageName<TRecord>(IEnumerable<(string Name, string? Storage)> properties)
    {
        var renamed = properties.FirstOrDefault(p => !string.IsNullOrEmpty(p.Storage));
        if (renamed.Storage != null)
            throw new NotSupportedException(
                $"'{typeof(TRecord).Name}.{renamed.Name}' declares StorageName '{renamed.Storage}'. DocumentDb takes a property's stored name from System.Text.Json — use [JsonPropertyName(\"{renamed.Storage}\")] (or the store's naming policy) instead.");
    }

    static PropertyInfo RequireProperty<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] TRecord>(string name, string role)
        => typeof(TRecord).GetProperty(name, BindingFlags.Public | BindingFlags.Instance)
            ?? throw new ArgumentException($"The definition names '{name}' as the {role} property, but '{typeof(TRecord).Name}' has no such public property.");

    /// <summary>
    /// MEVD's <see cref="DistanceFunction"/> string constants → <see cref="VectorDistance"/>. The similarity and
    /// distance spellings of a metric collapse onto the same enum value: DocumentDb picks the direction the
    /// backend natively reports, which is why <c>VectorResult.Score</c> is documented as provider-specific.
    /// </summary>
    public static VectorDistance MapDistance(string? distanceFunction) => distanceFunction switch
    {
        null or "" => VectorDistance.Cosine,
        DistanceFunction.CosineDistance or DistanceFunction.CosineSimilarity => VectorDistance.Cosine,
        DistanceFunction.EuclideanDistance => VectorDistance.Euclidean,
        DistanceFunction.DotProductSimilarity or DistanceFunction.NegativeDotProductSimilarity => VectorDistance.DotProduct,
        DistanceFunction.HammingDistance => VectorDistance.Hamming,
        _ => throw new NotSupportedException(
            $"Distance function '{distanceFunction}' has no DocumentDb equivalent. Supported: {DistanceFunction.CosineDistance}, {DistanceFunction.CosineSimilarity}, {DistanceFunction.EuclideanDistance}, {DistanceFunction.DotProductSimilarity}, {DistanceFunction.NegativeDotProductSimilarity}, {DistanceFunction.HammingDistance}.")
    };

    /// <summary>
    /// MEVD's <see cref="IndexKind"/> string constants → <see cref="VectorIndexKind"/>. Unset and
    /// <see cref="IndexKind.Dynamic"/> both map to <c>null</c>, which tells the provider to use its own default
    /// (DiskANN on Cosmos, HNSW elsewhere) rather than forcing a kind the backend may not have.
    /// </summary>
    public static VectorIndexKind? MapIndexKind(string? indexKind) => indexKind switch
    {
        null or "" or Microsoft.Extensions.VectorData.IndexKind.Dynamic => null,
        Microsoft.Extensions.VectorData.IndexKind.Hnsw => VectorIndexKind.Hnsw,
        Microsoft.Extensions.VectorData.IndexKind.Flat => VectorIndexKind.Flat,
        Microsoft.Extensions.VectorData.IndexKind.IvfFlat => VectorIndexKind.Ivf,
        Microsoft.Extensions.VectorData.IndexKind.DiskAnn => VectorIndexKind.DiskAnn,
        Microsoft.Extensions.VectorData.IndexKind.QuantizedFlat => VectorIndexKind.QuantizedFlat,
        _ => throw new NotSupportedException(
            $"Index kind '{indexKind}' has no DocumentDb equivalent. Supported: {Microsoft.Extensions.VectorData.IndexKind.Hnsw}, {Microsoft.Extensions.VectorData.IndexKind.Flat}, {Microsoft.Extensions.VectorData.IndexKind.IvfFlat}, {Microsoft.Extensions.VectorData.IndexKind.DiskAnn}, {Microsoft.Extensions.VectorData.IndexKind.QuantizedFlat}, {Microsoft.Extensions.VectorData.IndexKind.Dynamic}.")
    };
}
