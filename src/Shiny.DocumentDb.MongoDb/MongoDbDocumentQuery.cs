using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Text.Json.Serialization.Metadata;
using MongoDB.Bson;
using MongoDB.Driver;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.MongoDb;

/// <summary>
/// <see cref="IDocumentQuery{T}"/> for MongoDB. Predicates, ordering, and paging are translated into the find
/// command and run server-side, so <see cref="DocumentQueryBase{T}"/> re-applies nothing; grouping, cursor
/// pagination, and the aggregates that Mongo cannot express stay client-side over the filtered set.
/// </summary>
public class MongoDbDocumentQuery<T> : DocumentQueryBase<T> where T : class
{
    readonly MongoDbDocumentStore store;

    internal MongoDbDocumentQuery(MongoDbDocumentStore store, JsonTypeInfo<T>? typeInfo)
        : base(store.BuildQueryContext(typeInfo))
        => this.store = store;

    MongoDbDocumentQuery(MongoDbDocumentQuery<T> source) : base(source)
        => this.store = source.store;

    protected override DocumentQueryBase<T> Clone() => new MongoDbDocumentQuery<T>(this);

    protected override async Task<QueryExecution<T>> ExecuteAsync(QueryPlan<T> plan, CancellationToken ct)
    {
        var list = await this.store
            .ExecuteFindAsync(BuildFilter(plan), this.BuildSort(plan), plan.Skip, plan.Take, this.TypeInfo, ct)
            .ConfigureAwait(false);
        ComputedReadBack.Apply(list, this.store.Options.ResolveComputedMappings(typeof(T)));
        return QueryExecution<T>.Complete(list);
    }

    protected override Task<int> DeleteMatchingAsync(QueryPlan<T> plan, CancellationToken ct)
        => this.store.ExecuteDeleteAsync<T>(BuildFilter(plan), ct);

    protected override Task<int> SetPropertyMatchingAsync(QueryPlan<T> plan, string jsonPath, object? value, CancellationToken ct)
        => this.store.ExecuteUpdatePropertyAsync<T>(BuildFilter(plan), jsonPath, value, ct);

    // Mongo counts server-side; the rest of the aggregates materialize (the base default).
    protected override Task<long> CountCore(QueryPlan<T> plan, CancellationToken ct)
        => this.store.ExecuteCountAsync<T>(BuildFilter(plan), ct);

    protected override async Task<bool> AnyCore(QueryPlan<T> plan, CancellationToken ct)
        => await this.CountCore(plan, ct).ConfigureAwait(false) > 0;

    protected override IAsyncEnumerable<DocumentChange<T>> ObserveChanges(CancellationToken ct)
        => throw new NotSupportedException("Per-query change observation is not supported by MongoDbDocumentStore.");

    protected override Task<IReadOnlyList<FullTextResult<T>>> FullTextSearchCore(
        string searchText, int maxResults, Expression<Func<T, bool>>? filter, CancellationToken ct)
        => this.store.FullTextSearch(searchText, maxResults, filter, ct);

    protected override Task<IReadOnlyList<VectorResult<T>>> NearestVectorsCore(
        ReadOnlyMemory<float> query, int k, Expression<Func<T, bool>>? filter, CancellationToken ct)
        => this.store.NearestVectors(query, k, filter, ct);

    public override IGroupedDocumentQuery<T, object> GroupBy(string keyField)
        => throw new NotSupportedException(
            "String GroupBy(\"field\") is not supported on the MongoDB provider. Use the typed " +
            "GroupBy(keySelector).Select(g => …) form, or a relational store for the string grammar.");

    public override DocumentQueryString ToQueryString()
    {
        var registry = MongoDB.Bson.Serialization.BsonSerializer.SerializerRegistry;
        var serializer = registry.GetSerializer<BsonDocument>();
        var args = new RenderArgs<BsonDocument>(serializer, registry);

        var plan = this.BuildPlan();
        var filterDoc = BuildFilter(plan).Render(args);
        var sort = this.BuildSort(plan);

        // No ordering/pagination → return just the translated filter; otherwise render the full find command.
        BsonDocument query;
        if (sort == null && !plan.Skip.HasValue && !plan.Take.HasValue)
        {
            query = filterDoc;
        }
        else
        {
            query = new BsonDocument { { "filter", filterDoc } };
            if (sort != null)
                query.Add("sort", sort.Render(args));
            if (plan.Skip.HasValue)
                query.Add("skip", plan.Skip.Value);
            if (plan.Take.HasValue)
                query.Add("limit", plan.Take.Value);
        }

        var json = query.ToJson(new MongoDB.Bson.IO.JsonWriterSettings { OutputMode = MongoDB.Bson.IO.JsonOutputMode.RelaxedExtendedJson });
        return new DocumentQueryString(json, new Dictionary<string, object?>());
    }

    FilterDefinition<BsonDocument> BuildFilter(QueryPlan<T> plan)
    {
        if (plan.Predicates.Count == 0)
            return Builders<BsonDocument>.Filter.Empty;

        var translated = plan.Predicates
            .Select(p => MongoExpressionVisitor.Translate(p, this.Context.JsonOptions, this.TypeInfo))
            .ToList();

        return translated.Count == 1
            ? translated[0]
            : Builders<BsonDocument>.Filter.And(translated);
    }

    SortDefinition<BsonDocument>? BuildSort(QueryPlan<T> plan)
    {
        if (plan.OrderBy.Count == 0)
            return null;

        var sorts = new List<SortDefinition<BsonDocument>>();
        foreach (var (selector, desc) in plan.OrderBy)
        {
            var field = this.ResolveOrderField((Expression<Func<T, object>>)selector);
            sorts.Add(desc
                ? Builders<BsonDocument>.Sort.Descending(field)
                : Builders<BsonDocument>.Sort.Ascending(field));
        }
        return sorts.Count == 1 ? sorts[0] : Builders<BsonDocument>.Sort.Combine(sorts);
    }

    string ResolveOrderField<TVal>(Expression<Func<T, TVal>> selector)
    {
        var body = selector.Body;
        while (body is UnaryExpression { NodeType: ExpressionType.Convert } convert)
            body = convert.Operand;

        var parts = new List<string>();
        while (body is MemberExpression member)
        {
            var name = this.Context.JsonOptions.PropertyNamingPolicy?.ConvertName(member.Member.Name) ?? member.Member.Name;
            parts.Insert(0, name);
            body = member.Expression;
        }

        return $"{MongoFields.Data}.{string.Join(".", parts)}";
    }
}
