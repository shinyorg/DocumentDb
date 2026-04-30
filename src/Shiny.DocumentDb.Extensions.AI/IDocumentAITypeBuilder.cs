using System.Linq.Expressions;

namespace Shiny.DocumentDb.Extensions.AI;

/// <summary>
/// Per-type configuration for AI tool exposure. Lets callers override descriptions
/// pulled from <c>[Description]</c>/<c>[DisplayName]</c> attributes, or describe
/// types/properties that don't carry attributes.
/// </summary>
public interface IDocumentAITypeBuilder<T> where T : class
{
    /// <summary>Override the type-level description used in tool descriptions and JSON schema.</summary>
    IDocumentAITypeBuilder<T> Description(string description);

    /// <summary>Override the description for a specific property in the generated JSON schema.</summary>
    IDocumentAITypeBuilder<T> Property<TProp>(Expression<Func<T, TProp>> property, string description);

    /// <summary>
    /// Restrict which properties the LLM can see/filter on. If never called, all serializable properties are exposed.
    /// </summary>
    IDocumentAITypeBuilder<T> AllowProperties(params Expression<Func<T, object?>>[] properties);

    /// <summary>
    /// Hide one or more properties from the LLM's view. Useful for secrets or large blobs.
    /// </summary>
    IDocumentAITypeBuilder<T> IgnoreProperties(params Expression<Func<T, object?>>[] properties);

    /// <summary>Cap the maximum page size the LLM can request from query/aggregate tools. Default 100.</summary>
    IDocumentAITypeBuilder<T> MaxPageSize(int maxPageSize);
}
