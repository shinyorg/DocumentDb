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

        // A field the model was never shown is not one it may overwrite.
        var doc = JsonSerializer.Deserialize(this.StripHidden(json), this.Registration.JsonTypeInfo)
            ?? throw new InvalidOperationException("Failed to deserialize document.");

        // Enforce the non-removable filter on both sides of the write:
        //  1) the incoming document must satisfy it (the LLM cannot move a record out of scope), and
        //  2) the stored record being replaced must already be in scope (the LLM cannot overwrite a
        //     document it is not allowed to see).
        // Hidden properties also need the stored record: the update replaces the whole document, and the
        // model never had their values to send back.
        var scope = await this.ResolveScope(arguments, cancellationToken).ConfigureAwait(false);
        if (!scope.IsEmpty && !scope.Contains(doc))
            throw new InvalidOperationException(
                "The document violates the configured access policy for this type and was not updated.");

        if (!scope.IsEmpty || this.HasHiddenProperties)
        {
            var id = this.TryGetDocumentId(doc)
                ?? throw new InvalidOperationException(
                    "The document Id could not be resolved, so the stored record cannot be checked. " +
                    "Scoped updates, and updates of a type with hidden properties, require an 'Id' property " +
                    "on the document type that the model can see.");

            var existing = await this.Store
                .Get<T>(id, this.Registration.JsonTypeInfo, cancellationToken)
                .ConfigureAwait(false);

            if (existing is null || !scope.Contains(existing))
                return new { updated = false };

            this.CopyHiddenProperties(existing, doc);
        }

        await this.Store.Update(doc, this.Registration.JsonTypeInfo, cancellationToken).ConfigureAwait(false);
        return new { updated = true };
    }
}
