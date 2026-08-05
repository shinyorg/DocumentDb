using System.Text.Json;
using Microsoft.Extensions.AI;

namespace Shiny.DocumentDb.Extensions.AI.Internal;

sealed class JsonCollectionCountFunction : JsonCollectionFunctionBase
{
    public JsonCollectionCountFunction(
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
        var scope = await this.ResolveScope(arguments, cancellationToken).ConfigureAwait(false);
        var query = this.ApplyModelFilter(this.ScopedQuery(scope), AIArguments.GetJson(arguments, "filter"));
        var count = await query.Count(cancellationToken).ConfigureAwait(false);
        return new { count };
    }
}
