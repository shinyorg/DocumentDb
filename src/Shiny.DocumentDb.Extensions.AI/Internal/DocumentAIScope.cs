using System.Linq.Expressions;
using Microsoft.Extensions.AI;
using Shiny.DocumentDb.Internal.Query;

namespace Shiny.DocumentDb.Extensions.AI.Internal;

/// <summary>
/// A resolved scope delegate: static filters are baked at registration, request-resolved ones run here, once
/// per tool call.
/// </summary>
delegate ValueTask<Expression<Func<T, bool>>> DynamicDocumentFilter<T>(DocumentAIFilterContext context) where T : class;

/// <summary>The string-grammar twin, for the schema-free collection lane.</summary>
delegate ValueTask<string> DynamicCollectionFilter(DocumentAIFilterContext context);

/// <summary>
/// The non-removable filter set for a single tool call, already resolved. It carries the expressions (for
/// push-down into the store query) <b>and</b> their interpreted predicates (for the in-memory checks the
/// write paths make) so the two enforcement paths can never disagree — a disagreement is a scope bypass.
/// </summary>
readonly struct DocumentAIScope<T> where T : class
{
    readonly IReadOnlyList<Expression<Func<T, bool>>> expressions;
    readonly Func<T, bool>[] predicates;

    public DocumentAIScope(IReadOnlyList<Expression<Func<T, bool>>> expressions, Func<T, bool>[] predicates)
    {
        this.expressions = expressions;
        this.predicates = predicates;
    }

    public bool IsEmpty => this.predicates is null || this.predicates.Length == 0;

    /// <summary>AND-applies the scope to a store query, so it is pushed down to the provider.</summary>
    public IDocumentQuery<T> ApplyTo(IDocumentQuery<T> query)
    {
        if (this.expressions != null)
        {
            foreach (var expression in this.expressions)
                query = query.Where(expression);
        }
        return query;
    }

    /// <summary>Evaluates the scope in memory (compile-free, AOT-safe) — true when the document is inside it.</summary>
    public bool Contains(T document)
    {
        if (this.predicates != null)
        {
            foreach (var predicate in this.predicates)
            {
                if (!predicate(document))
                    return false;
            }
        }
        return true;
    }
}

/// <summary>Resolution shared by both lanes — one place that decides what "fail closed" means.</summary>
static class DocumentAIScopeResolver
{
    internal const string NoServicesMessage =
        "A request-resolved filter is configured for '{0}', but this tool call carries no " +
        "AIFunctionArguments.Services to resolve it from. The call is refused rather than run unscoped — " +
        "set Services on the arguments (the MCP server does this from the request scope).";

    public static async ValueTask<DocumentAIScope<T>> Resolve<T>(
        IReadOnlyList<Expression<Func<T, bool>>> staticExpressions,
        Func<T, bool>[] staticPredicates,
        IReadOnlyList<DynamicDocumentFilter<T>> dynamicFilters,
        AIFunctionArguments arguments,
        CancellationToken cancellationToken) where T : class
    {
        if (dynamicFilters.Count == 0)
            return new DocumentAIScope<T>(staticExpressions, staticPredicates);

        var services = arguments.Services
            ?? throw new InvalidOperationException(string.Format(NoServicesMessage, typeof(T).Name));

        var context = new DocumentAIFilterContext(services, arguments, cancellationToken);
        var expressions = new List<Expression<Func<T, bool>>>(staticExpressions.Count + dynamicFilters.Count);
        expressions.AddRange(staticExpressions);

        var predicates = new List<Func<T, bool>>(staticPredicates.Length + dynamicFilters.Count);
        predicates.AddRange(staticPredicates);

        foreach (var filter in dynamicFilters)
        {
            // A throwing filter (or a missing service) propagates and fails the tool call. Never caught here:
            // "run without the predicate" is the exact outcome this feature exists to prevent.
            var expression = await filter(context).ConfigureAwait(false)
                ?? throw new InvalidOperationException(
                    $"A request-resolved filter for '{typeof(T).Name}' returned null. Return a predicate " +
                    "(use x => false to deny everything) — null is not a valid scope.");

            expressions.Add(expression);
            predicates.Add(ExpressionInterpreter.Interpret(expression));
        }

        return new DocumentAIScope<T>(expressions, predicates.ToArray());
    }

    /// <summary>Collection lane: resolves the static + request-resolved clauses into one list.</summary>
    public static async ValueTask<IReadOnlyList<string>> Resolve(
        string collectionName,
        IReadOnlyList<string> staticFilters,
        IReadOnlyList<DynamicCollectionFilter> dynamicFilters,
        AIFunctionArguments arguments,
        CancellationToken cancellationToken)
    {
        if (dynamicFilters.Count == 0)
            return staticFilters;

        var services = arguments.Services
            ?? throw new InvalidOperationException(string.Format(NoServicesMessage, collectionName));

        var context = new DocumentAIFilterContext(services, arguments, cancellationToken);
        var clauses = new List<string>(staticFilters.Count + dynamicFilters.Count);
        clauses.AddRange(staticFilters);

        foreach (var filter in dynamicFilters)
        {
            var clause = await filter(context).ConfigureAwait(false);
            if (string.IsNullOrWhiteSpace(clause))
                throw new InvalidOperationException(
                    $"A request-resolved filter for collection '{collectionName}' returned an empty clause. " +
                    "Return a clause (use \"1 == 0\" to deny everything) — empty is not a valid scope.");

            clauses.Add(clause);
        }

        return clauses;
    }
}
