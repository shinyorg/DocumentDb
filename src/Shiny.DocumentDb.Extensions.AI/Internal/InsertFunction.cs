using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.Extensions.AI;

namespace Shiny.DocumentDb.Extensions.AI.Internal;

sealed class InsertFunction<T> : DocumentAIFunctionBase<T> where T : class
{
    public InsertFunction(
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
        if (!arguments.TryGetValue("document", out var raw) || raw is null)
            throw new InvalidOperationException("'document' argument is required.");

        var json = raw switch
        {
            JsonElement je => je.GetRawText(),
            string s       => s,
            _              => throw new InvalidOperationException("'document' must be a JSON object or string.")
        };

        var doc = JsonSerializer.Deserialize(json, this.Registration.JsonTypeInfo)
            ?? throw new InvalidOperationException("Failed to deserialize document.");

        await this.Store.Insert(doc, this.Registration.JsonTypeInfo, cancellationToken).ConfigureAwait(false);

        var resultJson = JsonSerializer.Serialize(doc, this.Registration.JsonTypeInfo);
        return new
        {
            inserted = true,
            document = JsonNode.Parse(resultJson)
        };
    }

    public static JsonElement BuildSchema(IReadOnlyList<DocumentField> fields, string? typeDescription)
    {
        var properties = new JsonObject();
        foreach (var f in fields)
            properties[f.JsonName] = SchemaBuilder.BuildFieldPropertySchema(f);

        var docSchema = new JsonObject
        {
            ["type"] = "object",
            ["properties"] = properties
        };
        if (!string.IsNullOrWhiteSpace(typeDescription))
            docSchema["description"] = typeDescription;

        var node = new JsonObject
        {
            ["type"] = "object",
            ["properties"] = new JsonObject { ["document"] = docSchema },
            ["required"] = new JsonArray("document")
        };
        return SchemaBuilder.ToJsonElement(node);
    }
}
