using System.Text.Json;
using Microsoft.Extensions.AI;

namespace Shiny.DocumentDb.Extensions.AI.Internal;

sealed class JsonCollectionAggregateFunction : JsonCollectionFunctionBase
{
    public JsonCollectionAggregateFunction(
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
        var function = GetArg<string>(arguments, "function")
            ?? throw new InvalidOperationException("'function' argument is required.");
        var fieldName = GetArg<string>(arguments, "field");

        var scope = await this.ResolveScope(arguments, cancellationToken).ConfigureAwait(false);
        var query = this.ApplyModelFilter(this.ScopedQuery(scope), AIArguments.GetJson(arguments, "filter"));

        if (string.Equals(function, "count", StringComparison.OrdinalIgnoreCase))
        {
            var count = await query.Count(cancellationToken).ConfigureAwait(false);
            return new { function = "count", value = (object)count };
        }

        if (string.IsNullOrWhiteSpace(fieldName))
            throw new InvalidOperationException($"Aggregate '{function}' requires a 'field' argument.");

        var field = this.Registration.ResolveField(fieldName!);
        if (this.Registration.Fields != null && !field.IsNumeric)
            throw new InvalidOperationException(
                $"Field '{fieldName}' is not numeric; only numeric fields are supported for sum/min/max/avg.");

        // The scalar aggregates extract numerically on every provider, so the plain path is what they want —
        // a :type hint belongs to the grammar, not here.
        var value = function.ToLowerInvariant() switch
        {
            "sum" => await query.Sum(field.Path, cancellationToken).ConfigureAwait(false),
            "min" => await query.Min(field.Path, cancellationToken).ConfigureAwait(false),
            "max" => await query.Max(field.Path, cancellationToken).ConfigureAwait(false),
            "avg" => await query.Average(field.Path, cancellationToken).ConfigureAwait(false),
            _ => throw new InvalidOperationException($"Unknown aggregate function '{function}'.")
        };

        return new { function = function.ToLowerInvariant(), field = fieldName, value };
    }
}
