using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.Extensions.AI;

namespace Shiny.DocumentDb.Extensions.AI.Internal;

sealed class GetByIdFunction<T> : DocumentAIFunctionBase<T> where T : class
{
    public GetByIdFunction(
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

        var doc = await this.Store
            .Get<T>(id, this.Registration.JsonTypeInfo, cancellationToken)
            .ConfigureAwait(false);

        if (doc is null)
            return new { found = false };

        var json = JsonSerializer.Serialize(doc, this.Registration.JsonTypeInfo);
        return new
        {
            found = true,
            document = JsonNode.Parse(json)
        };
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
                    ["description"] = "Document identifier (Guid, integer, or string — pass as string)."
                }
            },
            ["required"] = new JsonArray("id")
        };
        return SchemaBuilder.ToJsonElement(node);
    }
}
