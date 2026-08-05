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

        var scope = await this.ResolveScope(arguments, cancellationToken).ConfigureAwait(false);

        // The non-removable filter is a hard boundary: the LLM cannot insert a document that falls outside it.
        if (!scope.Contains(doc))
            throw new InvalidOperationException(
                "The document violates the configured access policy for this type and was not inserted.");

        await this.Store.Insert(doc, this.Registration.JsonTypeInfo, cancellationToken).ConfigureAwait(false);

        // Echo what was inserted, not a re-encrypted copy of it — the encrypting converters are symmetric, so
        // the store's own type info would turn every encrypted property into an envelope on the way back.
        var resultJson = JsonSerializer.Serialize(doc, DocumentEncryption.PlaintextView(this.Registration.JsonTypeInfo));
        return new
        {
            inserted = true,
            document = JsonNode.Parse(resultJson)
        };
    }
}
