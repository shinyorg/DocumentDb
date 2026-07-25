using System.Linq.Expressions;
using System.Text.Json.Serialization.Metadata;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.RavenDb;

/// <summary>
/// <see cref="IDocumentQuery{T}"/> for RavenDB. Documents of the type are loaded by id prefix and the plan —
/// predicates, ordering, paging — is applied by <see cref="DocumentQueryBase{T}"/>;
/// <see cref="ToQueryString"/> still renders the equivalent RQL.
/// </summary>
public class RavenDbDocumentQuery<T> : DocumentQueryBase<T> where T : class
{
    readonly RavenDbDocumentStore store;

    internal RavenDbDocumentQuery(RavenDbDocumentStore store, JsonTypeInfo<T>? typeInfo)
        : base(store.BuildQueryContext(typeInfo))
        => this.store = store;

    RavenDbDocumentQuery(RavenDbDocumentQuery<T> source) : base(source)
        => this.store = source.store;

    protected override DocumentQueryBase<T> Clone() => new RavenDbDocumentQuery<T>(this);

    protected override async Task<QueryExecution<T>> ExecuteAsync(QueryPlan<T> plan, CancellationToken ct)
    {
        var items = new List<T>();
        await foreach (var doc in this.store.LoadDocumentsAsync(this.TypeInfo, ct).ConfigureAwait(false))
            items.Add(doc);
        return QueryExecution<T>.Candidates(items);
    }

    protected override Task<int> DeleteMatchingAsync(QueryPlan<T> plan, CancellationToken ct)
        => this.store.DeleteWhereAsync(plan.CompilePredicate(), this.TypeInfo, ct);

    protected override Task<int> SetPropertyMatchingAsync(QueryPlan<T> plan, string jsonPath, object? value, CancellationToken ct)
        => this.store.UpdatePropertyWhereAsync(plan.CompilePredicate(), jsonPath, value, this.TypeInfo, ct);

    protected override IAsyncEnumerable<DocumentChange<T>> ObserveChanges(CancellationToken ct)
        => this.store.Broadcaster.Observe<T>(ct);

    public override DocumentQueryString ToQueryString()
    {
        var typeName = this.Context.TypeName;
        var where = $"startsWith(id(), '{typeName}/')";
        foreach (var predicate in this.BuildPredicatePlan().Predicates)
            where += $" and ({RavenExpressionVisitor.Translate(predicate, this.Context.JsonOptions, this.TypeInfo)})";

        var rql = $"from \"RavenDbDocuments\" where {where}";
        if (this.Ordering.Count > 0)
        {
            var terms = this.Ordering.Select(o =>
            {
                var field = ResolveOrderField((Expression<Func<T, object>>)o.Selector);
                return o.Descending ? $"{field} desc" : field;
            });
            rql += " order by " + String.Join(", ", terms);
        }
        return new DocumentQueryString(rql, new Dictionary<string, object?>());
    }

    string ResolveOrderField(Expression<Func<T, object>> selector)
    {
        var body = selector.Body;
        while (body is UnaryExpression { NodeType: ExpressionType.Convert } convert)
            body = convert.Operand;

        var parts = new List<string>();
        while (body is MemberExpression member)
        {
            var name = this.store.JsonOptions.PropertyNamingPolicy?.ConvertName(member.Member.Name) ?? member.Member.Name;
            parts.Insert(0, name);
            body = member.Expression;
        }
        return string.Join(".", parts);
    }
}
