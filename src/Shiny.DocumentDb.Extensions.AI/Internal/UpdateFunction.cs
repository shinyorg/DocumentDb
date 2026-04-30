using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.Extensions.AI;

namespace Shiny.DocumentDb.Extensions.AI.Internal;

sealed class UpdateFunction<T> : DocumentAIFunctionBase<T> where T : class
{
    public UpdateFunction(
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

        await this.Store.Update(doc, this.Registration.JsonTypeInfo, cancellationToken).ConfigureAwait(false);
        return new { updated = true };
    }

    public static JsonElement BuildSchema(IReadOnlyList<DocumentField> fields, string? typeDescription)
        => InsertFunction<T>.BuildSchema(fields, typeDescription);
}
