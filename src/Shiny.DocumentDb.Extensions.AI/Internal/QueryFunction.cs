using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.Extensions.AI;

namespace Shiny.DocumentDb.Extensions.AI.Internal;

sealed class QueryFunction<T> : DocumentAIFunctionBase<T> where T : class
{
    public QueryFunction(
        IDocumentStore store,
        DocumentAITypeRegistration<T> registration,
        IReadOnlyList<DocumentField> fields,
        string name,
        string description,
        JsonElement schema)
        : base(store, registration, fields, name, description, schema) { }

    protected override async ValueTask<object?> InvokeCoreAsync(
        AIFunctionArguments arguments,
        CancellationToken cancellationToken)
    {
        arguments.TryGetValue("filter", out var filterRaw);
        var filter = filterRaw is JsonElement je ? (JsonElement?)je : null;

        var orderBy = GetArg<string>(arguments, "orderBy");
        var orderDir = GetArg<string>(arguments, "orderDirection", "asc");
        var limit = GetArg<int>(arguments, "limit", 50);
        var offset = GetArg<int>(arguments, "offset", 0);

        if (limit <= 0) limit = 50;
        if (limit > this.Registration.MaxPageSize) limit = this.Registration.MaxPageSize;
        if (offset < 0) offset = 0;

        var query = this.ApplyFilters(this.Store.Query(this.Registration.JsonTypeInfo));

        var predicate = FilterTranslator.Translate<T>(filter, this.Fields);
        if (predicate != null)
            query = query.Where(predicate);

        if (!string.IsNullOrWhiteSpace(orderBy))
        {
            var orderExpr = OrderingHelper.BuildOrderBy<T>(orderBy!, this.Fields)
                ?? throw new InvalidOperationException($"Unknown or disallowed orderBy field '{orderBy}'.");
            query = string.Equals(orderDir, "desc", StringComparison.OrdinalIgnoreCase)
                ? query.OrderByDescending(orderExpr)
                : query.OrderBy(orderExpr);
        }

        query = query.Paginate(offset, limit);

        var array = new JsonArray();
        int count;

        // The tool result is JSON, and the scope is a query predicate rather than a post-filter, so nothing
        // here needs a T. Reading the stored bodies replaces deserialize → serialize → parse with one parse.
        if (query.SupportsRawJson)
        {
            var raw = await query.ToJsonList(cancellationToken).ConfigureAwait(false);
            foreach (var obj in raw)
                array.Add(obj);
            count = raw.Count;
        }
        else
        {
            // Encrypted properties — only the typed materialization decrypts, so the LLM would otherwise be
            // handed ciphertext.
            var results = await query.ToList(cancellationToken).ConfigureAwait(false);
            foreach (var doc in results)
            {
                var json = JsonSerializer.Serialize(doc, this.Registration.JsonTypeInfo);
                array.Add(JsonNode.Parse(json));
            }
            count = results.Count;
        }

        return new
        {
            count,
            offset,
            limit,
            documents = array
        };
    }
}
