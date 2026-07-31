using System.Text.Json;
using Microsoft.Extensions.AI;

namespace Shiny.DocumentDb.Extensions.AI.Internal;

sealed class JsonCollectionDeleteFunction : JsonCollectionFunctionBase
{
    public JsonCollectionDeleteFunction(
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

        // With a non-removable filter the LLM may only delete inside its scope; anything else is refused and
        // reported as if it were not there.
        if (this.HasFilters && !await this.IsInScope(id, cancellationToken).ConfigureAwait(false))
            return new { deleted = false };

        var deleted = await this.Collection.Remove(id, cancellationToken).ConfigureAwait(false);
        return new { deleted };
    }
}
