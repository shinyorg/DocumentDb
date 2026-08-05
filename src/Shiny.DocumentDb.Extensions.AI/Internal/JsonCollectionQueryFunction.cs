using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.Extensions.AI;

namespace Shiny.DocumentDb.Extensions.AI.Internal;

sealed class JsonCollectionQueryFunction : JsonCollectionFunctionBase
{
    public JsonCollectionQueryFunction(
        IDocumentStore store,
        DocumentAICollectionRegistration registration,
        string name,
        string description,
        JsonElement schema)
        : base(store, registration, name, description, schema) { }

    protected override async ValueTask<object?> InvokeCoreAsync(
        AIFunctionArguments arguments,
        CancellationToken cancellationToken)
    {
        var filter = AIArguments.GetJson(arguments, "filter");
        var orderBy = GetArg<string>(arguments, "orderBy");
        var orderDir = GetArg<string>(arguments, "orderDirection", "asc");
        var limit = GetArg<int>(arguments, "limit", 50);
        var offset = GetArg<int>(arguments, "offset", 0);

        if (limit <= 0) limit = 50;
        if (limit > this.Registration.MaxPageSize) limit = this.Registration.MaxPageSize;
        if (offset < 0) offset = 0;

        var scope = await this.ResolveScope(arguments, cancellationToken).ConfigureAwait(false);
        var query = this.ApplyModelFilter(this.ScopedQuery(scope), filter);

        if (!string.IsNullOrWhiteSpace(orderBy))
        {
            var field = this.Registration.ResolveField(orderBy!);
            // Open mode has no declared type for the sort key, so the model may name one; without it the
            // path sorts as text on every provider but SQLite.
            if (this.Registration.Fields is null)
            {
                var hint = DocumentFieldTypes.FromGrammarHint(GetArg<string>(arguments, "orderByType"));
                field = field with { Type = hint };
            }

            var descending = string.Equals(orderDir, "desc", StringComparison.OrdinalIgnoreCase);
            query = query.OrderBy(descending ? $"{field.HintedPath} desc" : field.HintedPath);
        }

        var results = await query.Paginate(offset, limit).ToList(cancellationToken).ConfigureAwait(false);

        var array = new JsonArray();
        foreach (var doc in results)
            array.Add((JsonNode)doc);

        return new
        {
            count = results.Count,
            offset,
            limit,
            documents = array
        };
    }
}
