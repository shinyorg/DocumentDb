using Microsoft.Extensions.AI;

namespace Shiny.DocumentDb.Extensions.AI.Internal;

static class DocumentAIFunctionFactory
{
    public static IEnumerable<AITool> Build<T>(IDocumentStore store, DocumentAITypeRegistration<T> reg) where T : class
    {
        var fields = SchemaBuilder.BuildFields(reg);
        var typeDescription = SchemaBuilder.ResolveTypeDescription(reg);
        var typeNoun = string.IsNullOrWhiteSpace(typeDescription)
            ? $"'{reg.Slug}' documents"
            : $"'{reg.Slug}' documents ({typeDescription})";

        if (reg.Capabilities.HasFlag(DocumentAICapabilities.Get))
        {
            yield return new GetByIdFunction<T>(
                store, reg, fields,
                name: $"{reg.Slug}_get_by_id",
                description: $"Fetch a single {typeNoun} by its identifier.",
                schema: GetByIdFunction<T>.BuildSchema());
        }

        if (reg.Capabilities.HasFlag(DocumentAICapabilities.Query))
        {
            yield return new QueryFunction<T>(
                store, reg, fields,
                name: $"{reg.Slug}_query",
                description:
                    $"Query {typeNoun} with a structured filter, optional sort and paging. " +
                    "Use the 'filter' parameter to restrict results.",
                schema: QueryFunction<T>.BuildSchema(fields, reg.MaxPageSize));
        }

        if (reg.Capabilities.HasFlag(DocumentAICapabilities.Count))
        {
            yield return new CountFunction<T>(
                store, reg, fields,
                name: $"{reg.Slug}_count",
                description: $"Count {typeNoun}, optionally restricted by a structured filter.",
                schema: CountFunction<T>.BuildSchema(fields));
        }

        if (reg.Capabilities.HasFlag(DocumentAICapabilities.Aggregate))
        {
            yield return new AggregateFunction<T>(
                store, reg, fields,
                name: $"{reg.Slug}_aggregate",
                description:
                    $"Compute a scalar aggregate (count/sum/min/max/avg) over {typeNoun}, " +
                    "optionally restricted by a structured filter.",
                schema: AggregateFunction<T>.BuildSchema(fields));
        }

        if (reg.Capabilities.HasFlag(DocumentAICapabilities.Insert))
        {
            yield return new InsertFunction<T>(
                store, reg, fields,
                name: $"{reg.Slug}_insert",
                description: $"Insert a new {typeNoun}.",
                schema: InsertFunction<T>.BuildSchema(fields, typeDescription));
        }

        if (reg.Capabilities.HasFlag(DocumentAICapabilities.Update))
        {
            yield return new UpdateFunction<T>(
                store, reg, fields,
                name: $"{reg.Slug}_update",
                description: $"Replace an existing {typeNoun} (must include the document's id).",
                schema: UpdateFunction<T>.BuildSchema(fields, typeDescription));
        }

        if (reg.Capabilities.HasFlag(DocumentAICapabilities.Delete))
        {
            yield return new DeleteFunction<T>(
                store, reg, fields,
                name: $"{reg.Slug}_delete",
                description: $"Delete a {typeNoun} by identifier.",
                schema: DeleteFunction<T>.BuildSchema());
        }
    }
}
