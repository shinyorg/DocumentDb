using Microsoft.Extensions.AI;

namespace Shiny.DocumentDb.Extensions.AI.Internal;

/// <summary>
/// The schema-free twin of <see cref="DocumentAIFunctionFactory"/>. Same tool names, same descriptions and
/// the same JSON schemas — a model that can drive a registered document type can drive a JSON collection
/// without being told which lane it is on.
/// </summary>
static class JsonCollectionFunctionFactory
{
    public static IEnumerable<AITool> Build(IDocumentStore store, DocumentAICollectionRegistration reg)
    {
        var fields = reg.SchemaFields;
        var noun = string.IsNullOrWhiteSpace(reg.Description)
            ? $"'{reg.Slug}' documents"
            : $"'{reg.Slug}' documents ({reg.Description})";

        if (reg.Capabilities.HasFlag(DocumentAICapabilities.Get))
        {
            yield return new JsonCollectionGetByIdFunction(
                store, reg,
                name: $"{reg.Slug}_get_by_id",
                description: $"Fetch a single {noun} by its identifier.",
                schema: SchemaBuilder.BuildIdSchema(
                    "Document identifier (Guid, integer, or string — pass as string)."));
        }

        if (reg.Capabilities.HasFlag(DocumentAICapabilities.Query))
        {
            yield return new JsonCollectionQueryFunction(
                store, reg,
                name: $"{reg.Slug}_query",
                description:
                    $"Query {noun} with a structured filter, optional sort and paging. " +
                    "Use the 'filter' parameter to restrict results.",
                schema: SchemaBuilder.BuildQuerySchema(fields, reg.MaxPageSize));
        }

        if (reg.Capabilities.HasFlag(DocumentAICapabilities.Count))
        {
            yield return new JsonCollectionCountFunction(
                store, reg,
                name: $"{reg.Slug}_count",
                description: $"Count {noun}, optionally restricted by a structured filter.",
                schema: SchemaBuilder.BuildCountSchema(fields));
        }

        if (reg.Capabilities.HasFlag(DocumentAICapabilities.Aggregate))
        {
            yield return new JsonCollectionAggregateFunction(
                store, reg,
                name: $"{reg.Slug}_aggregate",
                description:
                    $"Compute a scalar aggregate (count/sum/min/max/avg) over {noun}, " +
                    "optionally restricted by a structured filter.",
                schema: SchemaBuilder.BuildAggregateSchema(fields));
        }

        if (reg.Capabilities.HasFlag(DocumentAICapabilities.Insert))
        {
            yield return new JsonCollectionInsertFunction(
                store, reg,
                name: $"{reg.Slug}_insert",
                description: $"Insert a new {noun}.",
                schema: SchemaBuilder.BuildDocumentSchema(fields, reg.Description));
        }

        if (reg.Capabilities.HasFlag(DocumentAICapabilities.Update))
        {
            yield return new JsonCollectionUpdateFunction(
                store, reg,
                name: $"{reg.Slug}_update",
                description: $"Replace an existing {noun} (must include the document's id).",
                schema: SchemaBuilder.BuildDocumentSchema(fields, reg.Description));
        }

        if (reg.Capabilities.HasFlag(DocumentAICapabilities.Delete))
        {
            yield return new JsonCollectionDeleteFunction(
                store, reg,
                name: $"{reg.Slug}_delete",
                description: $"Delete a {noun} by identifier.",
                schema: SchemaBuilder.BuildIdSchema("Document identifier to delete."));
        }
    }
}
