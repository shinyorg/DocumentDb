using Json.Schema;

namespace Shiny.DocumentDb;

/// <summary>
/// Registry + options for JSON Schema validation. Map a compiled <see cref="JsonSchema"/> to a document
/// type (or supply a dynamic <see cref="Resolver"/>); the interceptor validates the exact JSON about to be
/// persisted (<c>ctx.GetJson()</c>) against it just before the write.
/// <para>
/// All <c>MapJsonSchema</c> overloads parse/load <b>once at registration</b> (fail-fast: a malformed schema
/// or missing file throws here, never on the first write). Schema property names must match the
/// <b>serialized</b> JSON names — camelCase by default (see <c>DocumentStoreOptions.JsonSerializerOptions</c>).
/// </para>
/// </summary>
public sealed class JsonSchemaOptions
{
    readonly Dictionary<Type, JsonSchema> schemas = new();

    /// <summary>Maps a pre-built <see cref="JsonSchema"/> to <typeparamref name="T"/>.</summary>
    public JsonSchemaOptions MapJsonSchema<T>(JsonSchema schema) where T : class
    {
        ArgumentNullException.ThrowIfNull(schema);
        this.schemas[typeof(T)] = schema;
        return this;
    }

    /// <summary>Maps a JSON-text schema to <typeparamref name="T"/> (parsed once via <see cref="JsonSchema.FromText"/>).</summary>
    public JsonSchemaOptions MapJsonSchema<T>(string schemaJson) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(schemaJson);
        return this.MapJsonSchema<T>(JsonSchema.FromText(schemaJson));
    }

    /// <summary>Maps a schema read from a stream (e.g. an embedded resource / MAUI app-package file) to <typeparamref name="T"/>.</summary>
    public JsonSchemaOptions MapJsonSchema<T>(Stream schemaJson) where T : class
    {
        ArgumentNullException.ThrowIfNull(schemaJson);
        using var reader = new StreamReader(schemaJson);
        return this.MapJsonSchema<T>(reader.ReadToEnd());
    }

    /// <summary>
    /// Maps a schema loaded from a file path to <typeparamref name="T"/>. Distinct from the JSON-text
    /// overload by name on purpose — a second <c>string</c> overload would be ambiguous. File paths are not
    /// portable to Blazor WASM / mobile bundled assets; use the <see cref="Stream"/> overload there.
    /// </summary>
    public JsonSchemaOptions MapJsonSchemaFromFile<T>(string path) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        return this.MapJsonSchema<T>(File.ReadAllText(path));
    }

    /// <summary>Optional dynamic fallback consulted when no static map entry exists for a type.</summary>
    public Func<Type, JsonSchema?>? Resolver { get; set; }

    /// <summary>
    /// When true (default), the <c>format</c> keyword (email, uuid, date-time, uri…) is <b>asserted</b>, not
    /// just annotated — draft 2020-12 treats it as annotation-only otherwise. Set false for spec-pure
    /// annotation behaviour (use <c>pattern</c> for real constraints instead).
    /// </summary>
    public bool EnableFormatAssertion { get; set; } = true;

    internal JsonSchema? Resolve(Type type)
        => this.schemas.TryGetValue(type, out var schema) ? schema : this.Resolver?.Invoke(type);
}
