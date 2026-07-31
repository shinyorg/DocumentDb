using System.Text.Json;
using Microsoft.Extensions.AI;

namespace Shiny.DocumentDb.Extensions.AI.Internal;

sealed class JsonCollectionUpdateFunction : JsonCollectionFunctionBase
{
    public JsonCollectionUpdateFunction(
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
        var document = JsonCollectionDocuments.ReadObject(arguments);
        var updated = await this.Collection.Update(document, cancellationToken: cancellationToken).ConfigureAwait(false);
        return new { updated = updated > 0 };
    }
}
