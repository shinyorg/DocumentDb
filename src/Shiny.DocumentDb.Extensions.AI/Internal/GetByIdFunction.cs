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

        var scope = await this.ResolveScope(arguments, cancellationToken).ConfigureAwait(false);
        var doc = await this.Store
            .Get<T>(id, this.Registration.JsonTypeInfo, cancellationToken)
            .ConfigureAwait(false);

        // A document outside the non-removable filter scope is invisible to the LLM — same as "not found".
        if (doc is null || !scope.Contains(doc))
            return new { found = false };

        // Plaintext view: the encrypting converters are symmetric, so serializing the materialized document
        // straight back through the store's type info would re-encrypt what Get just decrypted.
        var json = JsonSerializer.Serialize(doc, DocumentEncryption.PlaintextView(this.Registration.JsonTypeInfo));
        return new
        {
            found = true,
            document = JsonNode.Parse(json)
        };
    }
}
