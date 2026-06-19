using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb.Diagnostics;

/// <summary>
/// A telemetry decorator that wraps any <see cref="IDocumentStore"/> provider and emits an
/// OpenTelemetry-native metric and trace span per operation via <see cref="DocumentStoreMetrics"/>.
/// It is provider-agnostic — the <c>db.system.name</c> tag is derived from the wrapped store — and a
/// faithful stand-in: it also surfaces <see cref="ITemporalDocumentStore"/>, <see cref="IObservableDocumentStore"/>,
/// and <see cref="IChangeFeedDocumentStore"/> so a cast or pattern-match on the decorated store keeps working.
/// <para>
/// Coverage includes the fluent <see cref="Query{T}(JsonTypeInfo{T})"/> builder (its terminal operators
/// are instrumented via <see cref="InstrumentedDocumentQuery{T}"/>) and the operations applied by a
/// <see cref="UnitOfWork"/> on <see cref="UnitOfWork.SaveChanges"/> (each is recorded as a child span of
/// the transaction span). <c>NotifyOnChange</c> / <c>SubscribeChanges</c> are
/// long-lived subscriptions and are passed through without per-event telemetry.
/// </para>
/// </summary>
public sealed class InstrumentedDocumentStore : IDocumentStore, ITemporalDocumentStore, IObservableDocumentStore, IChangeFeedDocumentStore, IUnitOfWorkEngine
{
    readonly IDocumentStore inner;
    readonly OperationTracker tracker;
    readonly ITemporalDocumentStore? temporal;
    readonly IObservableDocumentStore? observable;
    readonly IChangeFeedDocumentStore? changeFeed;

    public InstrumentedDocumentStore(IDocumentStore inner, DocumentStoreMetrics metrics)
        : this(NotNull(inner), new OperationTracker(NotNull(metrics), ResolveSystem(inner)))
    {
    }

    InstrumentedDocumentStore(IDocumentStore inner, OperationTracker tracker)
    {
        this.inner = inner;
        this.tracker = tracker;
        this.temporal = inner as ITemporalDocumentStore;
        this.observable = inner as IObservableDocumentStore;
        this.changeFeed = inner as IChangeFeedDocumentStore;
    }

    /// <summary>The wrapped store, for callers that need provider-specific surface not on the interfaces.</summary>
    public IDocumentStore Inner => this.inner;

    static TValue NotNull<TValue>(TValue value, [CallerArgumentExpression(nameof(value))] string? name = null) where TValue : class
        => value ?? throw new ArgumentNullException(name);

    // db.system.name: the relational DocumentStore is generic over SQL backends, so its real engine
    // comes from the IDatabaseProvider; the document stores are named after the backend directly.
    static string ResolveSystem(IDocumentStore store)
        => store is DocumentStore ds
            ? Clean(ds.DatabaseProvider.GetType().Name, "DatabaseProvider")
            : Clean(store.GetType().Name, "DocumentStore");

    static string Clean(string name, string suffix)
        => (name.EndsWith(suffix, StringComparison.Ordinal) ? name[..^suffix.Length] : name).ToLowerInvariant();

    static string Coll<T>() => typeof(T).Name;

    ITemporalDocumentStore Temporal => this.temporal
        ?? throw new NotSupportedException($"The wrapped store '{this.inner.GetType().Name}' does not support temporal history.");

    // ── IDocumentStore ──────────────────────────────────────────────────

    public IDocumentQuery<T> Query<T>(JsonTypeInfo<T>? jsonTypeInfo = null) where T : class
        => new InstrumentedDocumentQuery<T>(this.inner.Query(jsonTypeInfo), this.tracker);

    public Task Insert<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.tracker.Track("insert", Coll<T>(), () => this.inner.Insert(document, jsonTypeInfo, cancellationToken));

    public Task<int> BatchInsert<T>(IEnumerable<T> documents, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.tracker.Track("batch_insert", Coll<T>(), () => this.inner.BatchInsert(documents, jsonTypeInfo, cancellationToken), r => r);

    public Task Update<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.tracker.Track("update", Coll<T>(), () => this.inner.Update(document, jsonTypeInfo, cancellationToken));

    public Task Upsert<T>(T patch, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.tracker.Track("upsert", Coll<T>(), () => this.inner.Upsert(patch, jsonTypeInfo, cancellationToken));

    public Task<bool> SetProperty<T>(object id, Expression<Func<T, object>> property, object? value, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.tracker.Track("set_property", Coll<T>(), () => this.inner.SetProperty(id, property, value, jsonTypeInfo, cancellationToken), r => r ? 1 : 0);

    public Task<bool> RemoveProperty<T>(object id, Expression<Func<T, object>> property, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.tracker.Track("remove_property", Coll<T>(), () => this.inner.RemoveProperty(id, property, jsonTypeInfo, cancellationToken), r => r ? 1 : 0);

    public Task<T?> Get<T>(object id, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.tracker.Track("get", Coll<T>(), () => this.inner.Get(id, jsonTypeInfo, cancellationToken), r => r is null ? 0 : 1);

    public Task<JsonPatchDocument<T>?> GetDiff<T>(object id, T modified, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.tracker.Track("get_diff", Coll<T>(), () => this.inner.GetDiff(id, modified, jsonTypeInfo, cancellationToken));

    public Task<IReadOnlyList<T>> Query<T>(string whereClause, JsonTypeInfo<T>? jsonTypeInfo = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class
        => this.tracker.Track("query", Coll<T>(), () => this.inner.Query(whereClause, jsonTypeInfo, parameters, cancellationToken), r => r.Count);

    public IAsyncEnumerable<T> QueryStream<T>(string whereClause, JsonTypeInfo<T>? jsonTypeInfo = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class
        => this.tracker.TrackStream("query_stream", Coll<T>(), this.inner.QueryStream<T>(whereClause, jsonTypeInfo, parameters, cancellationToken), cancellationToken);

    public Task<int> Count<T>(string? whereClause = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class
        => this.tracker.Track("count", Coll<T>(), () => this.inner.Count<T>(whereClause, parameters, cancellationToken), r => r);

    public Task<bool> Remove<T>(object id, CancellationToken cancellationToken = default) where T : class
        => this.tracker.Track("remove", Coll<T>(), () => this.inner.Remove<T>(id, cancellationToken), r => r ? 1 : 0);

    public Task<int> Clear<T>(CancellationToken cancellationToken = default) where T : class
        => this.tracker.Track("clear", Coll<T>(), () => this.inner.Clear<T>(cancellationToken), r => r);

    public UnitOfWork CreateUnitOfWork() => new(this);

    // The unit dispatches each op against an instrumented view of the transaction store, so each
    // operation inside SaveChanges is recorded as a child span of the surrounding "transaction" span.
    Task IUnitOfWorkEngine.RunUnitAsync(Func<IDocumentStore, CancellationToken, Task> work, CancellationToken cancellationToken)
        => this.tracker.Track("transaction", "(transaction)", () =>
            ((IUnitOfWorkEngine)this.inner).RunUnitAsync(
                (txInner, ct) => work(new InstrumentedDocumentStore(txInner, this.tracker), ct),
                cancellationToken));

    public bool SupportsSpatial => this.inner.SupportsSpatial;

    public Task<IReadOnlyList<SpatialResult<T>>> WithinRadius<T>(GeoPoint center, double radiusMeters, Expression<Func<T, bool>>? filter = null, CancellationToken cancellationToken = default) where T : class
        => this.tracker.Track("within_radius", Coll<T>(), () => this.inner.WithinRadius(center, radiusMeters, filter, cancellationToken), r => r.Count);

    public Task<IReadOnlyList<T>> WithinBoundingBox<T>(GeoBoundingBox box, Expression<Func<T, bool>>? filter = null, CancellationToken cancellationToken = default) where T : class
        => this.tracker.Track("within_bounding_box", Coll<T>(), () => this.inner.WithinBoundingBox(box, filter, cancellationToken), r => r.Count);

    public Task<IReadOnlyList<SpatialResult<T>>> NearestNeighbors<T>(GeoPoint center, int count, Expression<Func<T, bool>>? filter = null, CancellationToken cancellationToken = default) where T : class
        => this.tracker.Track("nearest_neighbors", Coll<T>(), () => this.inner.NearestNeighbors(center, count, filter, cancellationToken), r => r.Count);

    public bool SupportsVector => this.inner.SupportsVector;

    public Task<IReadOnlyList<VectorResult<T>>> NearestVectors<T>(ReadOnlyMemory<float> query, int k, Expression<Func<T, bool>>? filter = null, CancellationToken cancellationToken = default) where T : class
        => this.tracker.Track("nearest_vectors", Coll<T>(), () => this.inner.NearestVectors(query, k, filter, cancellationToken), r => r.Count);

    // ── ITemporalDocumentStore ──────────────────────────────────────────

    public Task<IReadOnlyList<DocumentVersion<T>>> History<T>(object id, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.tracker.Track("history", Coll<T>(), () => this.Temporal.History(id, jsonTypeInfo, cancellationToken), r => r.Count);

    public Task<T?> AsOf<T>(object id, DateTimeOffset asOf, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.tracker.Track("as_of", Coll<T>(), () => this.Temporal.AsOf(id, asOf, jsonTypeInfo, cancellationToken), r => r is null ? 0 : 1);

    public Task<IReadOnlyList<T>> AsOfAll<T>(DateTimeOffset asOf, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.tracker.Track("as_of_all", Coll<T>(), () => this.Temporal.AsOfAll<T>(asOf, jsonTypeInfo, cancellationToken), r => r.Count);

    public Task<IReadOnlyList<DocumentVersion<T>>> ChangesByActor<T>(string actor, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.tracker.Track("changes_by_actor", Coll<T>(), () => this.Temporal.ChangesByActor<T>(actor, jsonTypeInfo, cancellationToken), r => r.Count);

    public Task<IReadOnlyList<DocumentVersion<T>>> ChangesBetween<T>(DateTimeOffset from, DateTimeOffset to, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.tracker.Track("changes_between", Coll<T>(), () => this.Temporal.ChangesBetween<T>(from, to, jsonTypeInfo, cancellationToken), r => r.Count);

    public Task<T?> Restore<T>(object id, long version, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.tracker.Track("restore", Coll<T>(), () => this.Temporal.Restore<T>(id, version, jsonTypeInfo, cancellationToken), r => r is null ? 0 : 1);

    public Task<JsonPatchDocument<T>?> GetDiffBetween<T>(object id, long fromVersion, long toVersion, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.tracker.Track("get_diff_between", Coll<T>(), () => this.Temporal.GetDiffBetween<T>(id, fromVersion, toVersion, jsonTypeInfo, cancellationToken));

    // ── Optional capabilities: faithful pass-through (no per-event telemetry) ──

    public IAsyncEnumerable<DocumentChange<T>> NotifyOnChange<T>(CancellationToken cancellationToken = default) where T : class
        => (this.observable ?? throw new NotSupportedException($"The wrapped store '{this.inner.GetType().Name}' does not support change observation."))
            .NotifyOnChange<T>(cancellationToken);

    public Task<IAsyncDisposable> SubscribeChanges<T>(Func<DocumentChange<T>, CancellationToken, Task> onChange, CancellationToken cancellationToken = default) where T : class
        => (this.changeFeed ?? throw new NotSupportedException($"The wrapped store '{this.inner.GetType().Name}' does not support native change feeds."))
            .SubscribeChanges(onChange, cancellationToken);

    public void Dispose() => (this.inner as IDisposable)?.Dispose();
}
