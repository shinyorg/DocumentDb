using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.Extensions.AI;

namespace Shiny.DocumentDb.Extensions.AI.Internal;

sealed class JsonCollectionInsertFunction : JsonCollectionFunctionBase
{
    public JsonCollectionInsertFunction(
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
        var id = await this.Collection.Insert(document, cancellationToken).ConfigureAwait(false);

        // Insert stamps a generated id into the body, so this is the document as stored.
        return new { inserted = true, id, document };
    }
}

/// <summary>Reads the <c>document</c> argument the insert/update tools take.</summary>
static class JsonCollectionDocuments
{
    public static JsonObject ReadObject(AIFunctionArguments arguments)
    {
        if (!arguments.TryGetValue("document", out var raw) || raw is null)
            throw new InvalidOperationException("'document' argument is required.");

        var json = raw switch
        {
            JsonElement je => je.GetRawText(),
            string s => s,
            JsonObject o => o.ToJsonString(),
            _ => throw new InvalidOperationException("'document' must be a JSON object or string.")
        };

        var node = JsonNode.Parse(json)
            ?? throw new InvalidOperationException("'document' could not be parsed as JSON.");

        return node as JsonObject
            ?? throw new InvalidOperationException("'document' must be a JSON object.");
    }
}
