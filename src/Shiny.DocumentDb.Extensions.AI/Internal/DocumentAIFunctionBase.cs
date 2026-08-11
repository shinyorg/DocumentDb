using System.Reflection;
using System.Text.Json;
using Microsoft.Extensions.AI;
using Shiny.DocumentDb.Hosting;

namespace Shiny.DocumentDb.Extensions.AI.Internal;

abstract class DocumentAIFunctionBase<T> : AIFunction where T : class
{
    protected IDocumentStore Store { get; }
    protected DocumentAITypeRegistration<T> Registration { get; }
    protected IReadOnlyList<DocumentField> Fields { get; }

    readonly string name;
    readonly string description;
    readonly JsonElement schema;
    // Compile-free (AOT-safe) evaluators for the registered non-removable filters — same interpreter the
    // in-memory providers use. Built once per tool instance.
    readonly Func<T, bool>[] filterPredicates;

    protected DocumentAIFunctionBase(
        IDocumentStore store,
        DocumentAITypeRegistration<T> registration,
        IReadOnlyList<DocumentField> fields,
        string name,
        string description,
        JsonElement schema)
    {
        this.Store = store;
        this.Registration = registration;
        this.Fields = fields;
        this.name = name;
        this.description = description;
        this.schema = schema;
        this.filterPredicates = registration.Filters.Count == 0
            ? Array.Empty<Func<T, bool>>()
            : registration.Filters.Select(DocumentPredicate.Compile).ToArray();
    }

    public override string Name => this.name;
    public override string Description => this.description;
    public override JsonElement JsonSchema => this.schema;

    /// <summary>True when one or more non-removable <c>Where</c> filters are configured for this type.</summary>
    protected bool HasFilters => this.filterPredicates.Length > 0 || this.Registration.DynamicFilters.Count > 0;

    /// <summary>
    /// Resolves this call's scope: the static filters (interpreted once, in the constructor) plus any
    /// request-resolved ones, evaluated now. Call it <b>once</b> per invocation and use the result for both
    /// the query push-down and the in-memory checks — resolving twice invites the two enforcement paths
    /// disagreeing, which is a scope bypass.
    /// </summary>
    protected ValueTask<DocumentAIScope<T>> ResolveScope(AIFunctionArguments arguments, CancellationToken cancellationToken)
        => DocumentAIScopeResolver.Resolve(
            this.Registration.Filters,
            this.filterPredicates,
            this.Registration.DynamicFilters,
            arguments,
            cancellationToken);

    /// <summary>
    /// Reads the raw Id value off a materialized document via <c>JsonTypeInfo</c> (the "Id" convention),
    /// so the scoped update path can re-fetch the stored record. Returns null when the type exposes no
    /// property named "Id" (e.g. a custom-named mapped Id).
    /// </summary>
    protected object? TryGetDocumentId(T document)
    {
        foreach (var prop in this.Registration.JsonTypeInfo.Properties)
        {
            if (prop.AttributeProvider is MemberInfo mi && mi.Name == "Id")
                return prop.Get?.Invoke(document);
        }
        return null;
    }

    /// <summary>Reads a typed argument from the LLM-supplied bag, or returns <paramref name="fallback"/> if absent.</summary>
    protected static TValue? GetArg<TValue>(AIFunctionArguments arguments, string key, TValue? fallback = default)
        => AIArguments.Get(arguments, key, fallback);
}
