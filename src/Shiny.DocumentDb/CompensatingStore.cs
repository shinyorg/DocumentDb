using System.Linq.Expressions;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb;

/// <summary>
/// Shared base for the transaction-bound stores used by providers without single-node multi-document
/// transactions (MongoDB, Cosmos DB). It implements the compensating pattern: inserts are tracked and
/// deleted on rollback; reads and update/upsert/remove delegate straight to the live store and are NOT
/// compensated. Batch inserts go per-document so each is individually compensatable.
/// Subclasses supply the live <see cref="Inner"/> store, how to track an insert, and the rollback delete.
/// </summary>
abstract class CompensatingStore : IDocumentStore
{
    readonly List<(string typeName, string id)> inserted = new();

    /// <summary>The live store all non-insert operations delegate to.</summary>
    protected abstract IDocumentStore Inner { get; }

    /// <summary>Records an inserted document so it can be removed if the unit rolls back.</summary>
    protected void TrackInsert(string typeName, string id) => this.inserted.Add((typeName, id));

    /// <summary>Provider-specific delete of a tracked document during rollback.</summary>
    protected abstract Task DeleteTrackedAsync(string typeName, string id, CancellationToken ct);

    /// <summary>Compensates every tracked insert (best-effort).</summary>
    internal async Task RollbackAsync(CancellationToken ct)
    {
        foreach (var (typeName, id) in this.inserted)
        {
            try
            {
                await this.DeleteTrackedAsync(typeName, id, ct).ConfigureAwait(false);
            }
            catch
            {
                // Best-effort rollback.
            }
        }
    }

    /// <summary>Inserts and tracks a single document. Subclasses delegate to <see cref="Inner"/> then call <see cref="TrackInsert"/>.</summary>
    public abstract Task Insert<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class;

    public async Task<int> BatchInsert<T>(IEnumerable<T> documents, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var count = 0;
        foreach (var document in documents)
        {
            await this.Insert(document, jsonTypeInfo, cancellationToken).ConfigureAwait(false);
            count++;
        }
        return count;
    }

    // ── Everything else delegates to the live store (not compensated) ───
    public IDocumentQuery<T> Query<T>(JsonTypeInfo<T>? jsonTypeInfo = null) where T : class => this.Inner.Query(jsonTypeInfo);
    public Task Update<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class => this.Inner.Update(document, jsonTypeInfo, cancellationToken);
    public Task Upsert<T>(T patch, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class => this.Inner.Upsert(patch, jsonTypeInfo, cancellationToken);
    public Task<bool> SetProperty<T>(object id, Expression<Func<T, object>> property, object? value, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class => this.Inner.SetProperty(id, property, value, jsonTypeInfo, cancellationToken);
    public Task<bool> RemoveProperty<T>(object id, Expression<Func<T, object>> property, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class => this.Inner.RemoveProperty(id, property, jsonTypeInfo, cancellationToken);
    public Task<T?> Get<T>(object id, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class => this.Inner.Get(id, jsonTypeInfo, cancellationToken);
    public Task<JsonPatchDocument<T>?> GetDiff<T>(object id, T modified, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class => this.Inner.GetDiff(id, modified, jsonTypeInfo, cancellationToken);
    public Task<IReadOnlyList<T>> Query<T>(string whereClause, JsonTypeInfo<T>? jsonTypeInfo = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class => this.Inner.Query<T>(whereClause, jsonTypeInfo, parameters, cancellationToken);
    public IAsyncEnumerable<T> QueryStream<T>(string whereClause, JsonTypeInfo<T>? jsonTypeInfo = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class => this.Inner.QueryStream<T>(whereClause, jsonTypeInfo, parameters, cancellationToken);
    public Task<int> Count<T>(string? whereClause = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class => this.Inner.Count<T>(whereClause, parameters, cancellationToken);
    public Task<bool> Remove<T>(object id, CancellationToken cancellationToken = default) where T : class => this.Inner.Remove<T>(id, cancellationToken);
    public Task<int> Clear<T>(CancellationToken cancellationToken = default) where T : class => this.Inner.Clear<T>(cancellationToken);
    public UnitOfWork CreateUnitOfWork() => throw new InvalidOperationException("Nested units of work are not supported.");

    // ── Late-bound JSON lane — not available inside a unit of work ───────
    const string JsonLaneInUnitError =
        "The late-bound JSON lane (Insert/Update/Upsert/Get/Query by Type + JsonNode) is not available inside a UnitOfWork. Use the store directly.";
    public Task<int> Insert(Type type, JsonNode document, CancellationToken cancellationToken = default) => throw new NotSupportedException(JsonLaneInUnitError);
    public Task<int> Update(Type type, JsonNode document, CancellationToken cancellationToken = default) => throw new NotSupportedException(JsonLaneInUnitError);
    public Task<int> Upsert(Type type, JsonNode document, CancellationToken cancellationToken = default) => throw new NotSupportedException(JsonLaneInUnitError);
    public Task<JsonNode?> Get(Type type, object id, CancellationToken cancellationToken = default) => throw new NotSupportedException(JsonLaneInUnitError);
    public Task<IReadOnlyList<JsonNode>> Query(Type type, string whereClause, object? parameters = null, CancellationToken cancellationToken = default) => throw new NotSupportedException(JsonLaneInUnitError);
    public IAsyncEnumerable<JsonNode> QueryStream(Type type, string whereClause, object? parameters = null, CancellationToken cancellationToken = default) => throw new NotSupportedException(JsonLaneInUnitError);
}
