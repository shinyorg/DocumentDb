using System.Text.Json;
using Microsoft.Extensions.AI;

namespace Shiny.DocumentDb.Extensions.AI.Internal;

sealed class JsonCollectionGetByIdFunction : JsonCollectionFunctionBase
{
    public JsonCollectionGetByIdFunction(
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
        var id = GetArg<string>(arguments, "id")
            ?? throw new InvalidOperationException("'id' argument is required.");

        // A document outside the non-removable filter scope is invisible to the LLM — same as "not found".
        var scope = await this.ResolveScope(arguments, cancellationToken).ConfigureAwait(false);
        if (scope.Count > 0 && !await this.IsInScope(id, scope, cancellationToken).ConfigureAwait(false))
            return new { found = false };

        var doc = await this.Collection.Get(id, cancellationToken).ConfigureAwait(false);
        if (doc is null)
            return new { found = false };

        return new { found = true, document = doc };
    }
}
