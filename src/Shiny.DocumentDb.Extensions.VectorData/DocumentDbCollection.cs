using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.VectorData;

namespace Shiny.DocumentDb.Extensions.VectorData;

/// <summary>
/// One MEVD collection over a DocumentDb document type. The record <i>is</i> the document — there is no mapper
/// layer — and MEVD's filter is an <see cref="Expression{TDelegate}"/> of exactly the shape
/// <c>IDocumentStore.NearestVectors</c> already takes, so a filtered vector search passes straight through to the
/// engine and is pushed into the ANN query wherever the provider supports it.
/// </summary>
public sealed class DocumentDbCollection<TKey, TRecord> : VectorStoreCollection<TKey, TRecord>
    where TKey : notnull
    where TRecord : class
{
    readonly IDocumentStore store;
    readonly VectorRecordModel<TRecord> model;
    readonly IEmbeddingGenerator? embeddingGenerator;
    readonly VectorStoreCollectionMetadata metadata;

    // Null until the first upsert probes the provider; see Replace.
    bool? supportsReplaceUpsert;

    internal DocumentDbCollection(IDocumentStore store, VectorRecordModel<TRecord> model, IEmbeddingGenerator? embeddingGenerator)
    {
        // Data-model validation (MEVD connector requirement §5) — fail at construction, not at the first read.
        if (model.KeyType != typeof(TKey))
            throw new ArgumentException(
                $"'{typeof(TRecord).Name}.{model.KeyPropertyName}' is a {model.KeyType.Name}, so the collection key type must be {model.KeyType.Name}, not {typeof(TKey).Name}.",
                nameof(model));

        this.store = store;
        this.model = model;
        this.embeddingGenerator = embeddingGenerator;
        this.metadata = new VectorStoreCollectionMetadata
        {
            VectorStoreSystemName = DocumentDbVectorStore.SystemName,
            CollectionName = model.CollectionName
        };
    }

    /// <inheritdoc/>
    public override string Name => this.model.CollectionName;

    /// <inheritdoc/>
    /// <remarks>Always true — DocumentDb provisions a mapped type's storage when the store initializes.</remarks>
    public override Task<bool> CollectionExistsAsync(CancellationToken cancellationToken = default)
        => Task.FromResult(true);

    /// <inheritdoc/>
    /// <remarks>A no-op — the table and its vector index were provisioned when the store was built.</remarks>
    public override Task EnsureCollectionExistsAsync(CancellationToken cancellationToken = default)
        => Task.CompletedTask;

    /// <inheritdoc/>
    /// <remarks>Clears the type's documents; see <see cref="DocumentDbVectorStore.EnsureCollectionDeletedAsync"/>.</remarks>
    public override Task EnsureCollectionDeletedAsync(CancellationToken cancellationToken = default)
        => this.Guard("EnsureCollectionDeleted", () => this.store.Clear<TRecord>(cancellationToken));

    /// <inheritdoc/>
    public override async Task<TRecord?> GetAsync(TKey key, RecordRetrievalOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);

        var record = await this
            .Guard("Get", () => this.store.Get<TRecord>(key, cancellationToken: cancellationToken))
            .ConfigureAwait(false);

        return record == null ? null : this.Project(record, options?.IncludeVectors ?? false);
    }

    /// <inheritdoc/>
    /// <remarks>Missing keys are skipped, so the result is a subset rather than a positional match.</remarks>
    public override async IAsyncEnumerable<TRecord> GetAsync(
        IEnumerable<TKey> keys,
        RecordRetrievalOptions? options = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(keys);

        var includeVectors = options?.IncludeVectors ?? false;
        foreach (var key in keys)
        {
            var record = await this
                .Guard("Get", () => this.store.Get<TRecord>(key, cancellationToken: cancellationToken))
                .ConfigureAwait(false);

            if (record != null)
                yield return this.Project(record, includeVectors);
        }
    }

    /// <inheritdoc/>
    public override async IAsyncEnumerable<TRecord> GetAsync(
        Expression<Func<TRecord, bool>> filter,
        int top,
        FilteredRecordRetrievalOptions<TRecord>? options = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(filter);
        ArgumentOutOfRangeException.ThrowIfLessThan(top, 1);

        var query = this.store.Query<TRecord>().Where(filter);

        var sorts = options?.OrderBy?.Invoke(new FilteredRecordRetrievalOptions<TRecord>.OrderByDefinition()).Values;
        if (sorts != null)
        {
            foreach (var sort in sorts)
            {
                // The same expression, re-typed: MEVD annotates the selector's result as object?, the query
                // surface as object. Rebuilding the lambda keeps the nullability contracts honest either way.
                var selector = Expression.Lambda<Func<TRecord, object>>(sort.PropertySelector.Body, sort.PropertySelector.Parameters);
                query = sort.Ascending ? query.OrderBy(selector) : query.OrderByDescending(selector);
            }
        }

        var records = await this
            .Guard("Get", () => query.Paginate(options?.Skip ?? 0, top).ToList(cancellationToken))
            .ConfigureAwait(false);

        var includeVectors = options?.IncludeVectors ?? false;
        foreach (var record in records)
            yield return this.Project(record, includeVectors);
    }

    /// <inheritdoc/>
    /// <remarks>Deleting a key that is not there succeeds, as the contract requires.</remarks>
    public override Task DeleteAsync(TKey key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        return this.Guard("Delete", () => this.store.Remove<TRecord>(key, cancellationToken));
    }

    /// <inheritdoc/>
    public override Task DeleteAsync(IEnumerable<TKey> keys, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(keys);
        return this.Guard("Delete", () => this.store.BatchRemove<TRecord>(keys.Select(object (x) => x), cancellationToken));
    }

    /// <inheritdoc/>
    /// <remarks>
    /// A full-document replace, never a merge: MEVD upsert means "this is the record now", so a field the caller
    /// cleared has to be cleared in storage too — which DocumentDb's default merge-patch upsert would not do.
    /// </remarks>
    public override Task UpsertAsync(TRecord record, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(record);
        return this.Guard("Upsert", () => this.Replace(record, cancellationToken));
    }

    /// <inheritdoc/>
    /// <remarks>
    /// A loop of full-document replaces rather than <c>BatchUpsert</c>, which is a merge-patch batch.
    /// </remarks>
    public override async Task UpsertAsync(IEnumerable<TRecord> records, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(records);

        await this.Guard("Upsert", async () =>
        {
            foreach (var record in records)
            {
                ArgumentNullException.ThrowIfNull(record);
                await this.Replace(record, cancellationToken).ConfigureAwait(false);
            }
        }).ConfigureAwait(false);
    }

    /// <summary>
    /// Insert-or-replace. <c>Upsert(patchIfUpdate: false)</c> does this in one round trip, but only the relational
    /// providers implement it — the document-native ones throw. So probe it once per collection and fall back to
    /// the read-modify-write the relational stores run internally anyway, which keeps the connector's semantics
    /// identical on every backend.
    /// </summary>
    async Task Replace(TRecord record, CancellationToken cancellationToken)
    {
        if (this.supportsReplaceUpsert != false)
        {
            try
            {
                await this.store.Upsert(record, patchIfUpdate: false, cancellationToken: cancellationToken).ConfigureAwait(false);
                this.supportsReplaceUpsert = true;
                return;
            }
            catch (NotSupportedException)
            {
                // The default interface implementation throws before any I/O, so nothing was written.
                this.supportsReplaceUpsert = false;
            }
        }

        var key = this.model.GetKey(record);
        if (IsDefaultKey(key))
        {
            await this.store.Insert(record, cancellationToken: cancellationToken).ConfigureAwait(false);
            return;
        }

        var existing = await this.store.Get<TRecord>(key!, cancellationToken: cancellationToken).ConfigureAwait(false);
        if (existing == null)
            await this.store.Insert(record, cancellationToken: cancellationToken).ConfigureAwait(false);
        else
            await this.store.Update(record, cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    static bool IsDefaultKey(object? key) => key switch
    {
        null => true,
        string s => s.Length == 0,
        Guid g => g == Guid.Empty,
        int i => i == 0,
        long l => l == 0L,
        _ => false
    };

    /// <inheritdoc/>
    /// <remarks>
    /// <paramref name="searchValue"/> may be a <see cref="ReadOnlyMemory{T}"/> / <c>float[]</c> /
    /// <see cref="Embedding{T}"/> of <c>float</c>, or a <c>string</c> when an
    /// <see cref="IEmbeddingGenerator"/> is available. Results come back nearest-first on every provider; the raw
    /// <see cref="VectorSearchResult{TRecord}.Score"/> is the backend's own number and its direction and scale
    /// vary by provider (see <c>VectorResult&lt;T&gt;</c>), so rank on the ordering rather than the value.
    /// </remarks>
    public override async IAsyncEnumerable<VectorSearchResult<TRecord>> SearchAsync<TInput>(
        TInput searchValue,
        int top,
        VectorSearchOptions<TRecord>? options = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(searchValue);
        ArgumentOutOfRangeException.ThrowIfLessThan(top, 1);

        options ??= new VectorSearchOptions<TRecord>();
        ArgumentOutOfRangeException.ThrowIfNegative(options.Skip);
        this.RequireMappedVectorProperty(options.VectorProperty);

        var vector = await this.ResolveVector(searchValue, cancellationToken).ConfigureAwait(false);

        // Over-fetch by Skip so the window the caller asked for still holds `top` results.
        var hits = await this
            .Guard("Search", () => this.store.NearestVectors<TRecord>(vector, top + options.Skip, options.Filter, cancellationToken))
            .ConfigureAwait(false);

        foreach (var hit in hits.Skip(options.Skip))
        {
            // MEVD defines ScoreThreshold as a floor, which only reads as "more relevant" on providers whose score
            // is a similarity. On a provider reporting a distance, a floor keeps the *farthest* results — the
            // documented consequence of DocumentDb not normalizing scores across backends.
            if (options.ScoreThreshold is { } threshold && hit.Score < threshold)
                continue;

            yield return new VectorSearchResult<TRecord>(this.Project(hit.Document, options.IncludeVectors), hit.Score);
        }
    }

    /// <inheritdoc/>
    public override object? GetService(Type serviceType, object? serviceKey = null)
    {
        ArgumentNullException.ThrowIfNull(serviceType);
        if (serviceKey != null)
            return null;

        if (serviceType == typeof(VectorStoreCollectionMetadata))
            return this.metadata;

        if (serviceType == typeof(IDocumentStore))
            return this.store;

        return serviceType.IsInstanceOfType(this) ? this : null;
    }

    /// <summary>
    /// Strips the embedding unless the caller asked for it. The record is a freshly deserialized document, so
    /// clearing the property is safe and keeps the payload the caller sees honest about what was requested.
    /// </summary>
    TRecord Project(TRecord record, bool includeVectors)
    {
        if (!includeVectors)
            this.model.SetVector(record, default);
        return record;
    }

    void RequireMappedVectorProperty(Expression<Func<TRecord, object?>>? selector)
    {
        if (selector == null)
            return;

        var body = selector.Body;
        if (body is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } unary)
            body = unary.Operand;

        if (body is not MemberExpression member || !string.Equals(member.Member.Name, this.model.VectorPropertyName, StringComparison.Ordinal))
            throw new NotSupportedException(
                $"VectorSearchOptions.VectorProperty selects a property other than '{this.model.VectorPropertyName}'. DocumentDb maps one embedding per document type, so a record has a single searchable vector.");
    }

    async Task<ReadOnlyMemory<float>> ResolveVector<TInput>(TInput searchValue, CancellationToken cancellationToken) where TInput : notnull
    {
        var vector = searchValue switch
        {
            ReadOnlyMemory<float> memory => memory,
            float[] array => array.AsMemory(),
            Embedding<float> embedding => embedding.Vector,
            _ => (ReadOnlyMemory<float>?)null
        };

        if (vector != null)
            return this.RequireDimensions(vector.Value);

        if (searchValue is not string text)
            throw new NotSupportedException(
                $"A search value of type '{typeof(TInput).Name}' cannot be turned into an embedding. Pass a ReadOnlyMemory<float>, float[], Embedding<float>, or a string with an IEmbeddingGenerator registered.");

        if (this.embeddingGenerator is not IEmbeddingGenerator<string, Embedding<float>> generator)
            throw new InvalidOperationException(
                $"Searching '{this.Name}' by text needs an IEmbeddingGenerator<string, Embedding<float>>. Register one in DI, set VectorStoreCollectionDefinition.EmbeddingGenerator, or pass one to the DocumentDbVectorStore constructor.");

        var generated = await generator.GenerateAsync(text, cancellationToken: cancellationToken).ConfigureAwait(false);
        return this.RequireDimensions(generated.Vector);
    }

    ReadOnlyMemory<float> RequireDimensions(ReadOnlyMemory<float> vector)
        => vector.Length == this.model.Dimensions
            ? vector
            : throw new ArgumentException(
                $"The query vector has {vector.Length} dimensions but '{typeof(TRecord).Name}.{this.model.VectorPropertyName}' is mapped at {this.model.Dimensions}.");

    // MEVD asks connectors to surface backend failures as VectorStoreException, while leaving the caller's own
    // mistakes (bad arguments, cancellation) as themselves.
    async Task<T> Guard<T>(string operation, Func<Task<T>> action)
    {
        try
        {
            return await action().ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not (ArgumentException or OperationCanceledException or NotSupportedException or VectorStoreException))
        {
            throw new VectorStoreException($"'{operation}' on collection '{this.Name}' failed: {ex.Message}", ex)
            {
                VectorStoreSystemName = DocumentDbVectorStore.SystemName,
                CollectionName = this.Name,
                OperationName = operation
            };
        }
    }

    Task Guard(string operation, Func<Task> action)
        => this.Guard(operation, async () =>
        {
            await action().ConfigureAwait(false);
            return true;
        });
}
