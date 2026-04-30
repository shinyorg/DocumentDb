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

        var query = this.Store.Query(this.Registration.JsonTypeInfo);

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

        var results = await query.ToList(cancellationToken).ConfigureAwait(false);

        var array = new JsonArray();
        foreach (var doc in results)
        {
            var json = JsonSerializer.Serialize(doc, this.Registration.JsonTypeInfo);
            array.Add(JsonNode.Parse(json));
        }

        return new
        {
            count = results.Count,
            offset,
            limit,
            documents = array
        };
    }

    public static JsonElement BuildSchema(IReadOnlyList<DocumentField> fields, int maxPageSize)
    {
        var node = new JsonObject
        {
            ["type"] = "object",
            ["properties"] = new JsonObject
            {
                ["filter"] = SchemaBuilder.BuildFilterSchema(fields),
                ["orderBy"] = new JsonObject
                {
                    ["type"] = new JsonArray("string", "null"),
                    ["enum"] = BuildNullableEnum(fields),
                    ["description"] = "Optional field name to sort by."
                },
                ["orderDirection"] = new JsonObject
                {
                    ["type"] = "string",
                    ["enum"] = new JsonArray("asc", "desc"),
                    ["description"] = "Sort direction. Defaults to 'asc'."
                },
                ["limit"] = new JsonObject
                {
                    ["type"] = "integer",
                    ["minimum"] = 1,
                    ["maximum"] = maxPageSize,
                    ["description"] = $"Maximum number of documents to return (1-{maxPageSize}). Defaults to 50."
                },
                ["offset"] = new JsonObject
                {
                    ["type"] = "integer",
                    ["minimum"] = 0,
                    ["description"] = "Number of documents to skip. Defaults to 0."
                }
            }
        };
        return SchemaBuilder.ToJsonElement(node);
    }

    static JsonArray BuildNullableEnum(IReadOnlyList<DocumentField> fields)
    {
        var arr = new JsonArray();
        arr.Add((JsonNode?)null);
        foreach (var f in fields)
            arr.Add(f.JsonName);
        return arr;
    }
}
