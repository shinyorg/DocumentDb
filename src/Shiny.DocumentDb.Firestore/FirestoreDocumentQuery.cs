using System.Linq.Expressions;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization.Metadata;
using Google.Cloud.Firestore;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.Firestore;

/// <summary>
/// <see cref="IDocumentQuery{T}"/> for Google Cloud Firestore. Equality and range clauses over mapped fields
/// push down into the native query; the full predicate, ordering, and paging are then applied by
/// <see cref="DocumentQueryBase{T}"/>, so results are correct even when a clause could not be pushed.
/// <see cref="ToCursorPage"/> keeps using native Firestore keyset cursors.
/// </summary>
public class FirestoreDocumentQuery<T> : DocumentQueryBase<T> where T : class
{
    readonly FirestoreDocumentStore store;

    internal FirestoreDocumentQuery(FirestoreDocumentStore store, JsonTypeInfo<T>? typeInfo)
        : base(store.BuildQueryContext(typeInfo))
        => this.store = store;

    FirestoreDocumentQuery(FirestoreDocumentQuery<T> source) : base(source)
        => this.store = source.store;

    internal FirestoreDocumentStore Store => this.store;

    protected override DocumentQueryBase<T> Clone() => new FirestoreDocumentQuery<T>(this);

    protected override async Task<QueryExecution<T>> ExecuteAsync(QueryPlan<T> plan, CancellationToken ct)
    {
        var pushdown = this.store.BuildPushdown(plan.Predicates, this.TypeInfo);
        var items = new List<T>();
        await foreach (var doc in this.store.LoadDocumentsAsync(this.TypeInfo, pushdown, ct).ConfigureAwait(false))
            items.Add(doc);
        return QueryExecution<T>.Candidates(items);
    }

    protected override Task<int> DeleteMatchingAsync(QueryPlan<T> plan, CancellationToken ct)
        => this.store.DeleteWhereAsync(
            plan.CompilePredicate(),
            this.store.BuildPushdown(plan.Predicates, this.TypeInfo),
            this.TypeInfo,
            ct);

    protected override Task<int> SetPropertyMatchingAsync(QueryPlan<T> plan, string jsonPath, object? value, CancellationToken ct)
        => this.store.UpdatePropertyWhereAsync(
            plan.CompilePredicate(),
            jsonPath,
            value,
            this.store.BuildPushdown(plan.Predicates, this.TypeInfo),
            this.TypeInfo,
            ct);

    protected override IAsyncEnumerable<DocumentChange<T>> ObserveChanges(CancellationToken ct)
        => this.store.ListenChanges<T>(null, this.TypeInfo, ct);

    // ── Native keyset (cursor) pagination via StartAfter ────────────────

    public override Task<CursorPage<T>> ToCursorPage(string? cursor, int take, CancellationToken ct = default)
    {
        if (take <= 0)
            throw new ArgumentOutOfRangeException(nameof(take), "take must be greater than zero.");
        return this.store.Tracker.Track("query.to_cursor_page", typeof(T).Name, () => this.ToCursorPageImpl(cursor, take, ct), r => r.Items.Count);
    }

    async Task<CursorPage<T>> ToCursorPageImpl(string? cursor, int take, CancellationToken ct)
    {
        // Resolve the ordering fields (defaulting to document id) so the keyset order is total.
        var orderFields = new List<(string Path, bool Descending)>();
        foreach (var (selector, descending) in this.Ordering)
        {
            var path = this.ResolveJsonPath((Expression<Func<T, object>>)selector);
            orderFields.Add((path, descending));
        }

        Query query = this.store.GetCollection<T>();
        // Push equality clauses only — a range clause plus keyset ordering needs a matching composite index.
        var pushdown = this.store.BuildPushdown(this.BuildPredicatePlan().Predicates, this.TypeInfo);
        foreach (var clause in pushdown.Where(c => c.Op == FirestoreOp.Equal))
            query = query.WhereEqualTo(clause.Path, clause.Value);

        foreach (var (path, descending) in orderFields)
            query = descending ? query.OrderByDescending(path) : query.OrderBy(path);
        query = query.OrderBy(FieldPath.DocumentId);

        if (cursor != null)
        {
            var values = DecodeCursor(cursor, orderFields.Count);
            query = query.StartAfter(values);
        }
        query = query.Limit(take);

        var snapshot = await query.GetSnapshotAsync(ct).ConfigureAwait(false);

        var items = new List<T>(snapshot.Count);
        DocumentSnapshot? last = null;
        foreach (var doc in snapshot.Documents)
        {
            var model = this.store.DeserializeSnapshot(doc, this.TypeInfo);
            if (model == null)
                continue;
            items.Add(model);
            last = doc;
        }

        string? nextCursor = null;
        if (items.Count == take && last != null)
            nextCursor = EncodeCursor(last, orderFields);

        return new CursorPage<T>(items.AsReadOnly(), nextCursor);
    }

    static string EncodeCursor(DocumentSnapshot snapshot, List<(string Path, bool Descending)> orderFields)
    {
        var map = snapshot.ToDictionary();
        var arr = new JsonArray();
        foreach (var (path, _) in orderFields)
            arr.Add(FirestoreCursorValue(ExtractByPath(map, path)));
        arr.Add(snapshot.Id);
        var json = arr.ToJsonString();
        return Convert.ToBase64String(Encoding.UTF8.GetBytes(json));
    }

    static object[] DecodeCursor(string cursor, int orderFieldCount)
    {
        var json = Encoding.UTF8.GetString(Convert.FromBase64String(cursor));
        var arr = JsonNode.Parse(json)!.AsArray();
        var values = new object[arr.Count];
        for (var i = 0; i < arr.Count; i++)
        {
            var node = arr[i];
            // Last element is always the document id (string).
            if (i == arr.Count - 1)
            {
                values[i] = node!.GetValue<string>();
                continue;
            }
            values[i] = NodeToFirestoreValue(node);
        }
        return values;
    }

    static JsonNode? FirestoreCursorValue(object? value) => value switch
    {
        null => null,
        bool b => JsonValue.Create(b),
        string s => JsonValue.Create(s),
        long l => JsonValue.Create(l),
        int i => JsonValue.Create(i),
        double d => JsonValue.Create(d),
        _ => JsonValue.Create(value.ToString())
    };

    static object NodeToFirestoreValue(JsonNode? node)
    {
        if (node is not JsonValue v)
            return node?.ToJsonString() ?? "";
        var element = v.GetValue<JsonElement>();
        return element.ValueKind switch
        {
            JsonValueKind.String => element.GetString()!,
            JsonValueKind.Number => element.TryGetInt64(out var l) ? l : element.GetDouble(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            _ => element.GetRawText()
        };
    }

    static object? ExtractByPath(IDictionary<string, object> map, string path)
    {
        object? current = map;
        foreach (var part in path.Split('.'))
        {
            if (current is IDictionary<string, object> d && d.TryGetValue(part, out var next))
                current = next;
            else
                return null;
        }
        return current;
    }
}
