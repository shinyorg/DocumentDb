using System.Linq.Expressions;
using System.Text.Json.Serialization.Metadata;
using Shiny.DocumentDb.Internal.Query;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// Translates a <c>Where</c> predicate into a relational SQL fragment + bound parameters. Thin façade
/// over the shared query pipeline: <see cref="ExpressionLowerer"/> lowers the expression tree into the
/// backend-agnostic <see cref="Query.QueryNode"/> IR, and <see cref="SqlPredicateEmitter"/> renders it
/// using the provider's JSON SQL dialect.
/// </summary>
static class JsonExpressionVisitor
{
    public static (string WhereClause, Dictionary<string, object?> Parameters) Translate<T>(
        Expression<Func<T, bool>> predicate,
        JsonTypeInfo<T> jsonTypeInfo,
        IDatabaseProvider provider,
        FunctionTranslationRegistry? registry = null)
    {
        var node = ExpressionLowerer.Lower(predicate.Body, jsonTypeInfo.Options, jsonTypeInfo, registry);
        return SqlPredicateEmitter.Emit(node, provider);
    }
}
