using System.Linq.Expressions;
using System.Text.Json;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.LiteDb;

public class LiteDbDocumentStoreOptions
{
    readonly Dictionary<string, string> typeMappings = new();
    readonly HashSet<string> mappedCollectionNames = new(StringComparer.OrdinalIgnoreCase);
    readonly Dictionary<Type, string> idPropertyOverrides = new();

    public required string ConnectionString { get; set; }
    public TypeNameResolution TypeNameResolution { get; set; } = TypeNameResolution.ShortName;
    public JsonSerializerOptions? JsonSerializerOptions { get; set; }

    /// <summary>
    /// The name of the default shared document collection.
    /// Types not explicitly mapped via <see cref="MapTypeToCollection{T}"/> are stored here.
    /// Defaults to "documents".
    /// </summary>
    public string CollectionName { get; set; } = "documents";

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
    /// Maps a document type to its own dedicated collection.
    /// The collection name is auto-derived from the type name using the configured <see cref="TypeNameResolution"/>.
    /// </summary>
    public LiteDbDocumentStoreOptions MapTypeToCollection<T>() where T : class
    {
        var typeName = TypeNameResolver.Resolve(typeof(T), this.TypeNameResolution);
        return this.MapTypeToCollection<T>(typeName);
    }

    /// <summary>
    /// Maps a document type to its own dedicated collection with a custom Id property.
    /// </summary>
    public LiteDbDocumentStoreOptions MapTypeToCollection<T>(Expression<Func<T, object>> idProperty) where T : class
    {
        var typeName = TypeNameResolver.Resolve(typeof(T), this.TypeNameResolution);
        return this.MapTypeToCollection<T>(typeName, idProperty);
    }

    /// <summary>
    /// Maps a document type to a dedicated collection with the specified name.
    /// </summary>
    public LiteDbDocumentStoreOptions MapTypeToCollection<T>(string collectionName) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(collectionName);
        var typeName = TypeNameResolver.Resolve(typeof(T), this.TypeNameResolution);

        if (!this.mappedCollectionNames.Add(collectionName))
            throw new ArgumentException($"Collection '{collectionName}' is already mapped to another type.", nameof(collectionName));

        this.typeMappings[typeName] = collectionName;
        return this;
    }

    /// <summary>
    /// Maps a document type to a dedicated collection with the specified name and a custom Id property.
    /// </summary>
    public LiteDbDocumentStoreOptions MapTypeToCollection<T>(string collectionName, Expression<Func<T, object>> idProperty) where T : class
    {
        this.MapTypeToCollection<T>(collectionName);
        this.idPropertyOverrides[typeof(T)] = ExtractPropertyName(idProperty);
        return this;
    }

    internal string ResolveCollectionName(string typeName)
        => this.typeMappings.TryGetValue(typeName, out var collection) ? collection : this.CollectionName;

    internal string? ResolveIdPropertyName(Type type)
        => this.idPropertyOverrides.TryGetValue(type, out var name) ? name : null;

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
