namespace Shiny.DocumentDb.Extensions.AI;

/// <summary>
/// Per-collection configuration for AI tool exposure of a schema-free
/// <see cref="IJsonDocumentCollection"/>. The counterpart to <see cref="IDocumentAITypeBuilder{T}"/>, with
/// one structural difference: there is no CLR type to read metadata from, so the fields the LLM may see are
/// <b>declared</b> here rather than discovered.
/// </summary>
public interface IDocumentAICollectionBuilder
{
    /// <summary>Describes the collection in the generated tool descriptions and JSON schema.</summary>
    IDocumentAICollectionBuilder Description(string description);

    /// <summary>
    /// The JSON property carrying the document id. Defaults to <c>"id"</c>, matching
    /// <see cref="IDocumentStore.Collection(string, string)"/>.
    /// </summary>
    IDocumentAICollectionBuilder IdProperty(string idProperty);

    /// <summary>
    /// Declares a field the LLM may read, filter, sort and aggregate on. The declared set <b>is</b> the
    /// access boundary — a path that was never declared is refused, which is what
    /// <see cref="IDocumentAITypeBuilder{T}.AllowProperties"/> does for a typed registration.
    /// </summary>
    /// <param name="path">Dot-separated path into the document, e.g. <c>customer.name</c>.</param>
    /// <param name="type">The stored type. Becomes the JSON-schema type and the grammar's <c>:type</c> hint.</param>
    /// <param name="description">Optional description shown to the LLM.</param>
    IDocumentAICollectionBuilder Field(string path, DocumentFieldType type, string? description = null);

    /// <summary>
    /// Opts out of declared fields and lets the LLM name any path in the document. Intended for
    /// exploratory or trusted use over a collection whose shape genuinely is not known up front.
    /// </summary>
    /// <remarks>
    /// Three things are given up, all of them consequences of having nothing to enumerate: the model no
    /// longer sees which fields exist (it guesses paths), there is no field-level access boundary, and a
    /// sort key has no declared type — so it sorts as text unless the model supplies the optional type hint
    /// the schema exposes in this mode. Paths are still validated before they reach SQL.
    /// </remarks>
    IDocumentAICollectionBuilder AllowAnyField();

    /// <summary>Caps the maximum page size the LLM can request from the query tool. Default 100.</summary>
    IDocumentAICollectionBuilder MaxPageSize(int maxPageSize);

    /// <summary>
    /// Registers a fixed, server-side filter that constrains every tool for this collection. The LLM cannot
    /// see it, remove it, or override it — it is AND-combined with any filter the model supplies. Call
    /// multiple times to combine several conditions.
    /// </summary>
    /// <param name="filter">
    /// A string-grammar clause, e.g. <c>"tenantId == 'acme'"</c>. It is the same grammar
    /// <see cref="IJsonDocumentQuery.Where(string)"/> takes, including <c>:type</c> hints.
    /// </param>
    /// <remarks>
    /// <para>
    /// Enforced in SQL on every read: query/count/aggregate push it into the underlying query, and
    /// get-by-id/delete resolve the target through it first, so an out-of-scope id is indistinguishable from
    /// a missing document.
    /// </para>
    /// <para>
    /// <b>Not combinable with <see cref="DocumentAICapabilities.Insert"/> or
    /// <see cref="DocumentAICapabilities.Update"/></b> — registering both throws. A typed registration
    /// enforces the boundary on a write by evaluating the predicate against the materialized document; a
    /// schema-free body has no such evaluator, so the guarantee could not be kept and the combination is
    /// refused rather than silently weakened.
    /// </para>
    /// </remarks>
    IDocumentAICollectionBuilder Where(string filter);

    /// <summary>
    /// Registers a scope clause <b>resolved per tool call</b> from the call's services — the string-grammar
    /// twin of <see cref="IDocumentAITypeBuilder{T}.Where(Func{DocumentAIFilterContext, System.Linq.Expressions.Expression{Func{T, bool}}})"/>.
    /// AND-combined with every other filter and invisible to the model.
    /// </summary>
    /// <remarks>
    /// Fails <b>closed</b> exactly like the static form, and carries the same refusal to combine with
    /// <see cref="DocumentAICapabilities.Insert"/>/<see cref="DocumentAICapabilities.Update"/>: a schema-free
    /// body has no evaluator, so a write boundary could not be enforced.
    /// Build the clause from values you control; never concatenate model-supplied text into it.
    /// </remarks>
    IDocumentAICollectionBuilder Where(Func<DocumentAIFilterContext, string> filter);

    /// <summary>Asynchronous <see cref="Where(Func{DocumentAIFilterContext, string})"/>.</summary>
    IDocumentAICollectionBuilder Where(Func<DocumentAIFilterContext, ValueTask<string>> filter);

    /// <summary>Scope clause built from a service resolved out of the call's scope (<c>GetRequiredService</c>).</summary>
    IDocumentAICollectionBuilder Where<TService>(
        Func<TService, DocumentAIFilterContext, string> filter) where TService : notnull;

    /// <summary>Asynchronous <see cref="Where{TService}(Func{TService, DocumentAIFilterContext, string})"/>.</summary>
    IDocumentAICollectionBuilder Where<TService>(
        Func<TService, DocumentAIFilterContext, ValueTask<string>> filter) where TService : notnull;
}
