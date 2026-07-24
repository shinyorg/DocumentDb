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

        // Enforce the non-removable filter on both sides of the write:
        //  1) the incoming document must satisfy it (the LLM cannot move a record out of scope), and
        //  2) the stored record being replaced must already be in scope (the LLM cannot overwrite a
        //     document it is not allowed to see).
        if (this.HasFilters)
        {
            if (!this.InScope(doc))
                throw new InvalidOperationException(
                    "The document violates the configured access policy for this type and was not updated.");

            var id = this.TryGetDocumentId(doc)
                ?? throw new InvalidOperationException(
                    "A Where filter is configured but the document Id could not be resolved to enforce it. " +
                    "Scoped updates require an 'Id' property on the document type.");

            var existing = await this.Store
                .Get<T>(id, this.Registration.JsonTypeInfo, cancellationToken)
                .ConfigureAwait(false);

            if (existing is null || !this.InScope(existing))
                return new { updated = false };
        }

        await this.Store.Update(doc, this.Registration.JsonTypeInfo, cancellationToken).ConfigureAwait(false);
        return new { updated = true };
    }

    public static JsonElement BuildSchema(IReadOnlyList<DocumentField> fields, string? typeDescription)
        => InsertFunction<T>.BuildSchema(fields, typeDescription);
}
