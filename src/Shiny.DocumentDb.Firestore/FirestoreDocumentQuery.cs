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
/// <see cref="ToCursorPage"/> keeps using native Firestore keyset cursors, filling each page with the documents that pass
/// the full predicate.
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
            // A DocumentMetadata timestamp orders on its _meta envelope field — the body has no copy of it.
            var path = FirestoreExpressionVisitor.EnvelopePath(selector.Body, selector.Parameters[0])
                ?? this.ResolveJsonPath((Expression<Func<T, object>>)selector);
            orderFields.Add((path, descending));
        }

        Query query = this.store.GetCollection<T>();
        // Push equality clauses only — a range clause plus keyset ordering needs a matching composite index. Everything
        // else (range predicates, global query filters, computed properties) is applied below to each scanned document.
        var plan = this.BuildPredicatePlan();
        var pushdown = this.store.BuildPushdown(plan.Predicates, this.TypeInfo);
        foreach (var clause in pushdown.Where(c => c.Op == FirestoreOp.Equal))
            query = query.WhereEqualTo(clause.Path, clause.Value);

        foreach (var (path, descending) in orderFields)
            query = descending ? query.OrderByDescending(path) : query.OrderBy(path);
        query = query.OrderBy(FieldPath.DocumentId);

        var matches = plan.CompilePredicate();
        var applyComputed = this.Context.ApplyComputed;
        var items = new List<T>(take);
        DocumentSnapshot? lastReturned = null;

        // Fill the page: each native batch continues the keyset after the last document scanned, and only documents that
        // pass the full predicate count toward the page. The cursor is anchored on the last document *returned*, so a
        // rejected document after it is simply rescanned (and rejected again) by the next page.
        var batch = cursor != null
            ? query.StartAfter(DecodeCursor(cursor, orderFields.Count)).Limit(take)
            : query.Limit(take);
        var exhausted = false;
        while (items.Count < take && !exhausted)
        {
            var snapshot = await batch.GetSnapshotAsync(ct).ConfigureAwait(false);
            DocumentSnapshot? lastScanned = null;
            foreach (var doc in snapshot.Documents.TakeWhile(_ => items.Count < take))
            {
                lastScanned = doc;
                var model = this.store.DeserializeSnapshot(doc, this.TypeInfo);
                if (model != null)
                {
                    applyComputed?.Invoke(model);
                    if (matches(model))
                    {
                        items.Add(model);
                        lastReturned = doc;
                    }
                }
            }

            exhausted = snapshot.Count < take || lastScanned == null;
            if (!exhausted)
                batch = query.StartAfter(lastScanned!).Limit(take);
        }

        var nextCursor = items.Count == take && lastReturned != null
            ? EncodeCursor(lastReturned, orderFields)
            : null;

        return new CursorPage<T>(items.AsReadOnly(), nextCursor);
    }

    static string EncodeCursor(DocumentSnapshot snapshot, List<(string Path, bool Descending)> orderFields)
    {
        var map = snapshot.ToDictionary();
        var arr = new JsonArray();
        foreach (var (path, _) in orderFields)
            arr.Add(FirestoreCursorValue(ExtractByPath(map, path)));
        arr.Add((JsonNode)JsonValue.Create(snapshot.Id));
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
