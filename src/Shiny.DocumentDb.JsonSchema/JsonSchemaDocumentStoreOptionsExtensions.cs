using System.Runtime.CompilerServices;
using Json.Schema;

namespace Shiny.DocumentDb;

/// <summary>
/// Configure JSON Schema validation directly on any store's options — no DI required. Map a schema inside a
/// <c>ConfigureDocument</c> block; tune the registry on the options itself. Works against a hand-built store
/// (<c>new DocumentStore(options)</c>) as well as the DI path. Repeated <c>MapJsonSchema</c> calls accumulate
/// into a single validation interceptor attached to the options.
/// <code>
/// options.ConfigureDocument&lt;Order&gt;(cfg => cfg.MapJsonSchemaFromFile("order.schema.json"));
/// </code>
/// </summary>
public static class JsonSchemaDocumentStoreOptionsExtensions
{
    // One schema registry (and one attached interceptor) per options instance. The interceptor
    // holds a reference to this live registry, so schemas/settings added in any order before the store is
    // used are all seen.
    static readonly ConditionalWeakTable<IDocumentStoreOptions, JsonSchemaOptions> registries = new();

    static JsonSchemaOptions Registry(IDocumentStoreOptions options)
    {
        if (!registries.TryGetValue(options, out var schemaOptions))
        {
            schemaOptions = new JsonSchemaOptions();
            registries.Add(options, schemaOptions);
            options.AddInterceptor(new JsonSchemaInterceptor(schemaOptions));
        }
        return schemaOptions;
    }

    /// <summary>Maps a pre-built <see cref="JsonSchema"/> to this document type.</summary>
    public static DocumentTypeBuilder<T> MapJsonSchema<T>(this DocumentTypeBuilder<T> cfg, JsonSchema schema) where T : class
    {
        ArgumentNullException.ThrowIfNull(cfg);
        Registry(cfg.Options).MapJsonSchema<T>(schema);
        return cfg;
    }

    /// <summary>Maps a JSON-text schema to this document type (parsed once here).</summary>
    public static DocumentTypeBuilder<T> MapJsonSchema<T>(this DocumentTypeBuilder<T> cfg, string schemaJson) where T : class
    {
        ArgumentNullException.ThrowIfNull(cfg);
        Registry(cfg.Options).MapJsonSchema<T>(schemaJson);
        return cfg;
    }

    /// <summary>Maps a schema read from a stream (e.g. an embedded resource) to this document type.</summary>
    public static DocumentTypeBuilder<T> MapJsonSchema<T>(this DocumentTypeBuilder<T> cfg, Stream schemaJson) where T : class
    {
        ArgumentNullException.ThrowIfNull(cfg);
        Registry(cfg.Options).MapJsonSchema<T>(schemaJson);
        return cfg;
    }

    /// <summary>Maps a schema loaded from a file path to this document type.</summary>
    public static DocumentTypeBuilder<T> MapJsonSchemaFromFile<T>(this DocumentTypeBuilder<T> cfg, string path) where T : class
    {
        ArgumentNullException.ThrowIfNull(cfg);
        Registry(cfg.Options).MapJsonSchemaFromFile<T>(path);
        return cfg;
    }

    /// <summary>
    /// Tweaks the schema-validation registry for this store — e.g. <see cref="JsonSchemaOptions.EnableFormatAssertion"/>
    /// or a dynamic <see cref="JsonSchemaOptions.Resolver"/>. Operates on the same registry the
    /// <c>MapJsonSchema</c> calls populate.
    /// </summary>
    public static IDocumentStoreOptions ConfigureJsonSchemaValidation(this IDocumentStoreOptions options, Action<JsonSchemaOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(configure);
        configure(Registry(options));
        return options;
    }
}
