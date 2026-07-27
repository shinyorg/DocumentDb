using System.Linq.Expressions;
using System.Text.Json;
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
        FunctionTranslationRegistry? registry = null,
        IReadOnlyDictionary<string, ComputedMapping>? computed = null,
        string? tableName = null,
        string? fullTextTypeName = null,
        FullTextMapping? fullTextMapping = null)
        => Translate(predicate, jsonTypeInfo.Options, jsonTypeInfo, provider, registry, computed, tableName, fullTextTypeName, fullTextMapping);

    /// <summary>
    /// Overload for a schema-free collection: <paramref name="jsonTypeInfo"/> is <c>null</c> and the
    /// serializer options come in separately, because there is no document type to read them off.
    /// </summary>
    public static (string WhereClause, Dictionary<string, object?> Parameters) Translate<T>(
        Expression<Func<T, bool>> predicate,
        JsonSerializerOptions jsonOptions,
        JsonTypeInfo? jsonTypeInfo,
        IDatabaseProvider provider,
        FunctionTranslationRegistry? registry = null,
        IReadOnlyDictionary<string, ComputedMapping>? computed = null,
        string? tableName = null,
        string? fullTextTypeName = null,
        FullTextMapping? fullTextMapping = null)
    {
        var node = ExpressionLowerer.Lower(predicate.Body, jsonOptions, jsonTypeInfo, registry, computed);
        return SqlPredicateEmitter.Emit(node, provider, tableName, fullTextTypeName, fullTextMapping);
    }
}
