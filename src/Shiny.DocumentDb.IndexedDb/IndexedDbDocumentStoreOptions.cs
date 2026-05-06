using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Text.Json;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.IndexedDb;

public class IndexedDbDocumentStoreOptions
{
    readonly Dictionary<string, string> typeMappings = new();
    readonly HashSet<string> mappedStoreNames = new(StringComparer.OrdinalIgnoreCase);
    readonly Dictionary<Type, string> idPropertyOverrides = new();
    internal readonly Dictionary<Type, VersionMapping> versionMappings = new();

    /// <summary>
    /// The name of the IndexedDB database.
    /// </summary>
    public required string DatabaseName { get; set; }

    /// <summary>
    /// The IndexedDB database version. Increment when adding new object stores.
    /// </summary>
    public int Version { get; set; } = 1;

    public TypeNameResolution TypeNameResolution { get; set; } = TypeNameResolution.ShortName;
    public JsonSerializerOptions? JsonSerializerOptions { get; set; }

    /// <summary>
    /// The name of the default shared object store.
    /// Types not explicitly mapped via <see cref="MapTypeToStore{T}"/> are stored here.
    /// Defaults to "documents".
    /// </summary>
    public string StoreName { get; set; } = "documents";

    /// <summary>
    /// When false, calling a reflection-based overload (without JsonTypeInfo&lt;T&gt;) throws an
    /// InvalidOperationException if the type cannot be resolved from the configured TypeInfoResolver.
    /// Set to false in AOT deployments to get clear errors instead of hard-to-diagnose trimming failures.
    /// Defaults to true.
    /// </summary>
    public bool UseReflectionFallback { get; set; } = true;

    /// <summary>
    /// Optional callback invoked with diagnostic messages.
    /// </summary>
    public Action<string>? Logging { get; set; }

    /// <summary>
    /// Maps a document type to its own dedicated object store.
    /// </summary>
    public IndexedDbDocumentStoreOptions MapTypeToStore<T>() where T : class
    {
        var typeName = TypeNameResolver.Resolve(typeof(T), this.TypeNameResolution);
        return this.MapTypeToStore<T>(typeName);
    }

    /// <summary>
    /// Maps a document type to its own dedicated object store with a custom Id property.
    /// </summary>
    public IndexedDbDocumentStoreOptions MapTypeToStore<T>(Expression<Func<T, object>> idProperty) where T : class
    {
        var typeName = TypeNameResolver.Resolve(typeof(T), this.TypeNameResolution);
        return this.MapTypeToStore<T>(typeName, idProperty);
    }

    /// <summary>
    /// Maps a document type to a dedicated object store with the specified name.
    /// </summary>
    public IndexedDbDocumentStoreOptions MapTypeToStore<T>(string storeName) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(storeName);
        var typeName = TypeNameResolver.Resolve(typeof(T), this.TypeNameResolution);

        if (!this.mappedStoreNames.Add(storeName))
            throw new ArgumentException($"Store '{storeName}' is already mapped to another type.", nameof(storeName));

        this.typeMappings[typeName] = storeName;
        return this;
    }

    /// <summary>
    /// Maps a document type to a dedicated object store with the specified name and a custom Id property.
    /// </summary>
    public IndexedDbDocumentStoreOptions MapTypeToStore<T>(string storeName, Expression<Func<T, object>> idProperty) where T : class
    {
        this.MapTypeToStore<T>(storeName);
        this.idPropertyOverrides[typeof(T)] = ExtractPropertyName(idProperty);
        return this;
    }

    internal string ResolveStoreName(string typeName)
        => this.typeMappings.TryGetValue(typeName, out var store) ? store : this.StoreName;

    internal string? ResolveIdPropertyName(Type type)
        => this.idPropertyOverrides.TryGetValue(type, out var name) ? name : null;

    /// <summary>
    /// Maps a version property on a document type for optimistic concurrency.
    /// </summary>
    [UnconditionalSuppressMessage("Trimming", "IL2075", Justification = "Property is resolved by name from a user-provided expression.")]
    public IndexedDbDocumentStoreOptions MapVersionProperty<T>(Expression<Func<T, int>> property) where T : class
    {
        var body = property.Body;
        if (body is not MemberExpression member)
            throw new ArgumentException("Expression must be a simple property access.", nameof(property));

        var propertyName = member.Member.Name;
        var propInfo = typeof(T).GetProperty(propertyName)
            ?? throw new ArgumentException($"Property '{propertyName}' not found on type '{typeof(T).Name}'.");

        this.versionMappings[typeof(T)] = new VersionMapping
        {
            DocumentType = typeof(T),
            PropertyName = propertyName,
            GetVersion = obj => (int)propInfo.GetValue(obj)!,
            SetVersion = (obj, v) => propInfo.SetValue(obj, v)
        };
        return this;
    }

    /// <summary>
    /// Maps a version property on a document type for optimistic concurrency. AOT-safe overload.
    /// </summary>
    public IndexedDbDocumentStoreOptions MapVersionProperty<T>(string propertyName, Func<T, int> getter, Action<T, int> setter) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(propertyName);
        this.versionMappings[typeof(T)] = new VersionMapping
        {
            DocumentType = typeof(T),
            PropertyName = propertyName,
            GetVersion = obj => getter((T)obj),
            SetVersion = (obj, v) => setter((T)obj, v)
        };
        return this;
    }

    internal VersionMapping? ResolveVersionMapping(Type type)
        => this.versionMappings.TryGetValue(type, out var mapping) ? mapping : null;

    internal void ResolveVersionJsonPaths(JsonSerializerOptions jsonOptions)
    {
        foreach (var mapping in this.versionMappings.Values)
        {
            if (mapping.JsonPath != null!)
                continue;
            var jsonName = jsonOptions.PropertyNamingPolicy?.ConvertName(mapping.PropertyName) ?? mapping.PropertyName;
            mapping.JsonPath = jsonName;
        }
    }

    internal IEnumerable<string> GetAllStoreNames()
    {
        yield return this.StoreName;
        foreach (var store in this.typeMappings.Values)
            yield return store;
    }

    static string ExtractPropertyName<T>(Expression<Func<T, object>> expression)
    {
        var body = expression.Body;
        if (body is UnaryExpression { NodeType: ExpressionType.Convert } unary)
            body = unary.Operand;

        if (body is MemberExpression member)
            return member.Member.Name;

        throw new ArgumentException(
            "Expression must be a simple property access (e.g., x => x.MyId).",
            nameof(expression));
    }
}
