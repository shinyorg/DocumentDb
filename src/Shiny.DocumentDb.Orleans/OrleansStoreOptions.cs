using System.Text.Json;

namespace Shiny.DocumentDb.Orleans;

/// <summary>
/// Shared configuration for the DocumentDb-backed Orleans system stores (reminders, clustering,
/// grain directory). Supply a relational <see cref="DatabaseProvider"/> (the built-in path — the
/// document type/version mappings are wired for you) or a <see cref="StoreFactory"/> returning a
/// fully-configured <see cref="IDocumentStore"/> (the generic escape hatch).
/// </summary>
public abstract class OrleansStoreOptions
{
    /// <summary>Relational backend provider. Ignored when <see cref="StoreFactory"/> is set.</summary>
    public IDatabaseProvider? DatabaseProvider { get; set; }

    /// <summary>
    /// Generic escape hatch: returns a fully-configured <see cref="IDocumentStore"/> with the required
    /// document type(s) and version property mapped (see the per-feature mapping helper).
    /// </summary>
    public Func<IServiceProvider, IDocumentStore>? StoreFactory { get; set; }

    /// <summary>Table/collection name override. Each feature supplies its own default.</summary>
    public string? TableName { get; set; }

    /// <summary>JSON options for document serialization. When null, a permissive default is used.</summary>
    public JsonSerializerOptions? JsonSerializerOptions { get; set; }

    /// <summary>
    /// When <c>false</c>, document (de)serialization must resolve a source-generated
    /// <see cref="System.Text.Json.Serialization.Metadata.JsonTypeInfo{T}"/> — a clear exception is thrown
    /// instead of falling back to reflection. Defaults to <c>true</c>. The feature's own envelope types are
    /// always source-generated; this governs the reflection last-resort for app-supplied state.
    /// </summary>
    public bool UseReflectionFallback { get; set; } = true;
}

static class OrleansDocumentStore
{
    /// <summary>
    /// Resolves the <see cref="IDocumentStore"/> for a feature: uses the caller's
    /// <see cref="OrleansStoreOptions.StoreFactory"/> if present, otherwise builds a relational
    /// <see cref="DocumentStore"/> and applies <paramref name="map"/> (which registers the feature's
    /// document type(s) + version property on the supplied table).
    /// </summary>
    public static IDocumentStore Build(
        OrleansStoreOptions options,
        IServiceProvider services,
        string defaultTable,
        Action<DocumentStoreOptions, string> map
    )
    {
        if (options.StoreFactory is not null)
            return options.StoreFactory(services);

        if (options.DatabaseProvider is null)
            throw new InvalidOperationException(
                "An Orleans DocumentDb store requires either a DatabaseProvider or a StoreFactory to be configured.");

        var table = options.TableName ?? defaultTable;
        var dso = new DocumentStoreOptions
        {
            DatabaseProvider = options.DatabaseProvider,
            TableName = table,
            // The internal envelope types are source-generated, so the store can serialize them
            // reflection-free regardless of what the caller configured for grain state.
            JsonSerializerOptions = OrleansSystemJson.StoreOptions(options.JsonSerializerOptions),
            UseReflectionFallback = options.UseReflectionFallback
        };

        map(dso, table);
        return new DocumentStore(dso);
    }
}
