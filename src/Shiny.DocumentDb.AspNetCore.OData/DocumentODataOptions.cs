using Microsoft.OData.Edm;
using Microsoft.OData.ModelBuilder;
using Shiny.DocumentDb.OData;

namespace Shiny.DocumentDb.AspNetCore.OData;

/// <summary>
/// Collects the document types exposed as OData entity sets and builds the shared EDM model. Registered
/// via <c>AddDocumentODataEndpoints</c>; resolved by the endpoint mapping to parse incoming query options.
/// </summary>
public sealed class DocumentODataOptions
{
    readonly ODataConventionModelBuilder builder = new();
    readonly Dictionary<Type, string> entitySets = new();
    readonly Dictionary<Type, Action<ODataQueryPolicy>> policyOverrides = new();
    IEdmModel? model;

    /// <summary>
    /// The governance policy applied to every entity set unless overridden per set. Configure it to set
    /// API-wide defaults (e.g. a default/max page size). Defaults are permissive — no limits.
    /// </summary>
    public ODataQueryPolicy DefaultPolicy { get; } = new();

    /// <summary>Sets API-wide governance defaults (result caps, allowed options, allowlists).</summary>
    public DocumentODataOptions ConfigureDefaultPolicy(Action<ODataQueryPolicy> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        configure(this.DefaultPolicy);
        return this;
    }

    /// <summary>
    /// The effective policy for <typeparamref name="T"/>: a clone of <see cref="DefaultPolicy"/> with any
    /// per-entity-set override applied on top.
    /// </summary>
    public ODataQueryPolicy GetPolicy<T>() where T : class
    {
        var policy = new ODataQueryPolicy(this.DefaultPolicy);
        if (this.policyOverrides.TryGetValue(typeof(T), out var configure))
            configure(policy);
        return policy;
    }

    /// <summary>
    /// Registers a document type as an OData entity set with the given path name (e.g. <c>"customers"</c>).
    /// </summary>
    /// <param name="name">The entity-set name used in the OData path and <c>@odata.context</c>.</param>
    /// <param name="keyProperty">
    /// The EDM key property name. Defaults to <c>Id</c>; pass the store's configured id property
    /// (<c>MapIdProperty&lt;T&gt;</c>) when it differs so <c>@odata.id</c> links resolve correctly.
    /// </param>
    public DocumentODataOptions EntitySet<T>(string name, string keyProperty = "Id") where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        this.model = null; // invalidate the cached model

        // Touch the entity type so the convention builder registers it.
        _ = this.builder.EntityType<T>();

        // The convention builder auto-detects an `Id` key. For a custom id property, register it
        // explicitly (via the non-generic config's HasKey(PropertyInfo)) so the EDM key matches the
        // store's MapIdProperty<T> configuration.
        if (!keyProperty.Equals("Id", StringComparison.OrdinalIgnoreCase))
        {
            var keyPi = typeof(T).GetProperty(keyProperty)
                ?? throw new ArgumentException(
                    $"Key property '{keyProperty}' not found on '{typeof(T).Name}'.", nameof(keyProperty));
            this.builder.AddEntityType(typeof(T)).HasKey(keyPi);
        }

        this.builder.EntitySet<T>(name);
        this.entitySets[typeof(T)] = name;
        return this;
    }

    /// <summary>
    /// Registers a document type as an OData entity set with a per-entity-set governance policy. The
    /// <paramref name="configurePolicy"/> action runs against a clone of <see cref="DefaultPolicy"/>, so it
    /// only needs to express the differences from the API-wide defaults.
    /// </summary>
    public DocumentODataOptions EntitySet<T>(
        string name, Action<ODataQueryPolicy> configurePolicy, string keyProperty = "Id") where T : class
    {
        ArgumentNullException.ThrowIfNull(configurePolicy);
        this.EntitySet<T>(name, keyProperty);
        this.policyOverrides[typeof(T)] = configurePolicy;
        return this;
    }

    /// <summary>The OData entity-set name registered for <typeparamref name="T"/>.</summary>
    public string GetEntitySetName<T>() where T : class
        => this.entitySets.TryGetValue(typeof(T), out var name)
            ? name
            : throw new InvalidOperationException(
                $"No OData entity set registered for '{typeof(T).Name}'. Call EntitySet<{typeof(T).Name}>(...) in AddDocumentODataEndpoints.");

    /// <summary>The lazily-built, shared EDM model across all registered entity sets.</summary>
    public IEdmModel GetEdmModel() => this.model ??= this.builder.GetEdmModel();
}
