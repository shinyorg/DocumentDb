using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.Extensions.AI;

namespace Shiny.DocumentDb.Extensions.AI.Internal;

sealed class DeleteFunction<T> : DocumentAIFunctionBase<T> where T : class
{
    public DeleteFunction(
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
        var id = GetArg<string>(arguments, "id")
            ?? throw new InvalidOperationException("'id' argument is required.");

        // With a non-removable filter, the LLM may only delete documents inside its scope. Fetch first and
        // refuse to touch anything the filter rejects (indistinguishable from "not found").
        if (this.HasFilters)
        {
            var doc = await this.Store
                .Get<T>(id, this.Registration.JsonTypeInfo, cancellationToken)
                .ConfigureAwait(false);

            if (doc is null || !this.InScope(doc))
                return new { deleted = false };
        }

        var deleted = await this.Store.Remove<T>(id, cancellationToken).ConfigureAwait(false);
        return new { deleted };
    }
}
