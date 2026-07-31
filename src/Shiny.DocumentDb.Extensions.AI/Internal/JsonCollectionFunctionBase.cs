using System.Text.Json;
using Microsoft.Extensions.AI;

namespace Shiny.DocumentDb.Extensions.AI.Internal;

/// <summary>
/// Base for the tools generated over a schema-free <see cref="IJsonDocumentCollection"/>. The typed lane's
/// counterpart (<see cref="DocumentAIFunctionBase{T}"/>) can evaluate its registered filters in memory
/// against a materialized document; here there is no CLR instance and no evaluator, so scope is resolved in
/// SQL — the filters are pushed into a query and the target is looked up through it.
/// </summary>
abstract class JsonCollectionFunctionBase : AIFunction
{
    protected IDocumentStore Store { get; }
    protected DocumentAICollectionRegistration Registration { get; }

    readonly string name;
    readonly string description;
    readonly JsonElement schema;

    protected JsonCollectionFunctionBase(
        IDocumentStore store,
        DocumentAICollectionRegistration registration,
        string name,
        string description,
        JsonElement schema)
    {
        this.Store = store;
        this.Registration = registration;
        this.name = name;
        this.description = description;
        this.schema = schema;
    }

    public override string Name => this.name;
    public override string Description => this.description;
    public override JsonElement JsonSchema => this.schema;

    /// <summary>Opens the target collection. Throws on a provider that does not implement the JSON lane.</summary>
    protected IJsonDocumentCollection Collection => this.Registration.Open(this.Store);

    /// <summary>True when one or more non-removable <c>Where</c> filters are configured.</summary>
    protected bool HasFilters => this.Registration.HasFilters;

    /// <summary>A query with the registered non-removable filters already AND-combined into it.</summary>
    protected IJsonDocumentQuery ScopedQuery()
    {
        var query = this.Collection.Query();
        foreach (var filter in this.Registration.Filters)
            query = query.Where(filter);
        return query;
    }

    /// <summary>AND-combines the model's structured filter onto a query, with values bound as parameters.</summary>
    protected IJsonDocumentQuery ApplyModelFilter(IJsonDocumentQuery query, JsonElement? filter)
    {
        var clause = JsonFilterTranslator.Translate(filter, this.Registration);
        return clause?.ApplyTo(query) ?? query;
    }

    /// <summary>
    /// Whether <paramref name="id"/> resolves to a document inside the configured scope. Only meaningful
    /// when filters are configured — the callers skip it otherwise.
    /// </summary>
    protected Task<bool> IsInScope(string id, CancellationToken cancellationToken)
    {
        var clause = new GrammarFilter();
        clause.Literal(this.Registration.IdProperty);
        clause.Literal(" == ");
        clause.Arg(id);
        return clause.ApplyTo(this.ScopedQuery()).Any(cancellationToken);
    }

    /// <summary>Reads a typed argument from the LLM-supplied bag.</summary>
    protected static TValue? GetArg<TValue>(AIFunctionArguments arguments, string key, TValue? fallback = default)
        => AIArguments.Get(arguments, key, fallback);
}
