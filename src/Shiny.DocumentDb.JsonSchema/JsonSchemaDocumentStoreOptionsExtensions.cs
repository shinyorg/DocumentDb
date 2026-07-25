using System.Runtime.CompilerServices;
using Json.Schema;

namespace Shiny.DocumentDb;

/// <summary>
/// Configure JSON Schema validation directly on any store's options — no DI required.
/// Works against a hand-built store (<c>new DocumentStore(options)</c>) as well as the DI path. Repeated
/// <c>MapJsonSchema</c> calls accumulate into a single validation interceptor attached to the options.
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

    /// <summary>Maps a pre-built <see cref="JsonSchema"/> to <typeparamref name="T"/>.</summary>
    public static IDocumentStoreOptions MapJsonSchema<T>(this IDocumentStoreOptions options, JsonSchema schema) where T : class
    {
        ArgumentNullException.ThrowIfNull(options);
        Registry(options).MapJsonSchema<T>(schema);
        return options;
    }

    /// <summary>Maps a JSON-text schema to <typeparamref name="T"/> (parsed once here).</summary>
    public static IDocumentStoreOptions MapJsonSchema<T>(this IDocumentStoreOptions options, string schemaJson) where T : class
    {
        ArgumentNullException.ThrowIfNull(options);
        Registry(options).MapJsonSchema<T>(schemaJson);
        return options;
    }

    /// <summary>Maps a schema read from a stream (e.g. an embedded resource) to <typeparamref name="T"/>.</summary>
    public static IDocumentStoreOptions MapJsonSchema<T>(this IDocumentStoreOptions options, Stream schemaJson) where T : class
    {
        ArgumentNullException.ThrowIfNull(options);
        Registry(options).MapJsonSchema<T>(schemaJson);
        return options;
    }

    /// <summary>Maps a schema loaded from a file path to <typeparamref name="T"/>.</summary>
    public static IDocumentStoreOptions MapJsonSchemaFromFile<T>(this IDocumentStoreOptions options, string path) where T : class
    {
        ArgumentNullException.ThrowIfNull(options);
        Registry(options).MapJsonSchemaFromFile<T>(path);
        return options;
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
