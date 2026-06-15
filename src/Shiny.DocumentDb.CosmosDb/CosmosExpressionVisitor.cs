using System.Linq.Expressions;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Shiny.DocumentDb.Internal.Query;

namespace Shiny.DocumentDb.CosmosDb;

/// <summary>
/// Translates LINQ <c>Where</c> predicates into Cosmos NoSQL WHERE fragments. Thin façade over the shared
/// query pipeline: <see cref="ExpressionLowerer"/> lowers the expression tree into the backend-agnostic IR
/// and <see cref="CosmosSqlEmitter"/> renders Cosmos SQL (property access maps to <c>c.data.{path}</c>).
/// </summary>
internal static class CosmosExpressionVisitor
{
    internal static (string sql, Dictionary<string, object?> parameters) Translate<T>(
        Expression<Func<T, bool>> expression,
        JsonSerializerOptions jsonOptions,
        JsonTypeInfo<T>? typeInfo,
        int paramStart = 0) where T : class
    {
        JsonTypeInfo rootInfo = typeInfo ?? jsonOptions.GetTypeInfo(typeof(T));
        var node = ExpressionLowerer.Lower(expression.Body, jsonOptions, rootInfo);
        return CosmosSqlEmitter.Emit(node, paramStart);
    }
}
