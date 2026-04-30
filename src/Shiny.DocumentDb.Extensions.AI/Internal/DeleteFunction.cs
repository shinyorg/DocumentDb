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

        var deleted = await this.Store.Remove<T>(id, cancellationToken).ConfigureAwait(false);
        return new { deleted };
    }

    public static JsonElement BuildSchema()
    {
        var node = new JsonObject
        {
            ["type"] = "object",
            ["properties"] = new JsonObject
            {
                ["id"] = new JsonObject
                {
                    ["type"] = "string",
                    ["description"] = "Document identifier to delete."
                }
            },
            ["required"] = new JsonArray("id")
        };
        return SchemaBuilder.ToJsonElement(node);
    }
}
