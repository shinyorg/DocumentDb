using System.Linq.Expressions;
using System.Text.Json;
using Microsoft.Azure.Cosmos;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.CosmosDb;

public class CosmosDbDocumentStoreOptions
{
    readonly Dictionary<string, string> typeMappings = new();
    readonly HashSet<string> mappedContainerNames = new(StringComparer.OrdinalIgnoreCase);
    readonly Dictionary<Type, string> idPropertyOverrides = new();

    public required string ConnectionString { get; set; }
    public required string DatabaseName { get; set; }
    public string ContainerName { get; set; } = "documents";
    public TypeNameResolution TypeNameResolution { get; set; } = TypeNameResolution.ShortName;
    public JsonSerializerOptions? JsonSerializerOptions { get; set; }

    /// <summary>
    /// When false, calling a reflection-based overload (without JsonTypeInfo&lt;T&gt;) throws an
    /// InvalidOperationException if the type cannot be resolved from the configured TypeInfoResolver.
    /// Defaults to true.
    /// </summary>
    public bool UseReflectionFallback { get; set; } = true;

    /// <summary>
    /// Optional callback invoked with diagnostic messages (Cosmos SQL queries).
    /// </summary>
    public Action<string>? Logging { get; set; }

    /// <summary>
    /// Default throughput for auto-created containers. Defaults to 400 RU/s.
    /// </summary>
    public int DefaultThroughput { get; set; } = 400;

    /// <summary>
    /// Optional pre-configured CosmosClient. When null, a new client is created from <see cref="ConnectionString"/>.
    /// </summary>
    public CosmosClient? CosmosClient { get; set; }

    /// <summary>
    /// Maps a document type to its own dedicated container.
    /// </summary>
    public CosmosDbDocumentStoreOptions MapTypeToContainer<T>() where T : class
    {
        var typeName = TypeNameResolver.Resolve(typeof(T), this.TypeNameResolution);
        return this.MapTypeToContainer<T>(typeName);
    }

    /// <summary>
    /// Maps a document type to its own dedicated container with a custom Id property.
    /// </summary>
    public CosmosDbDocumentStoreOptions MapTypeToContainer<T>(Expression<Func<T, object>> idProperty) where T : class
    {
        var typeName = TypeNameResolver.Resolve(typeof(T), this.TypeNameResolution);
        return this.MapTypeToContainer<T>(typeName, idProperty);
    }

    /// <summary>
    /// Maps a document type to a dedicated container with the specified name.
    /// </summary>
    public CosmosDbDocumentStoreOptions MapTypeToContainer<T>(string containerName) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(containerName);
        var typeName = TypeNameResolver.Resolve(typeof(T), this.TypeNameResolution);

        if (!this.mappedContainerNames.Add(containerName))
            throw new ArgumentException($"Container '{containerName}' is already mapped to another type.", nameof(containerName));

        this.typeMappings[typeName] = containerName;
        return this;
    }

    /// <summary>
    /// Maps a document type to a dedicated container with the specified name and a custom Id property.
    /// </summary>
    public CosmosDbDocumentStoreOptions MapTypeToContainer<T>(string containerName, Expression<Func<T, object>> idProperty) where T : class
    {
        this.MapTypeToContainer<T>(containerName);
        this.idPropertyOverrides[typeof(T)] = ExtractPropertyName(idProperty);
        return this;
    }

    internal string ResolveContainerName(string typeName)
        => this.typeMappings.TryGetValue(typeName, out var container) ? container : this.ContainerName;

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
