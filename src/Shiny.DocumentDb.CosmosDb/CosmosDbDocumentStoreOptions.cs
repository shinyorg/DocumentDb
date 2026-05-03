using System.Diagnostics.CodeAnalysis;
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
    internal readonly Dictionary<Type, CosmosDbSpatialMapping> spatialMappings = new();

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

    /// <summary>
    /// Declares that type T has a GeoPoint property to be used for spatial queries.
    /// The property will be serialized as GeoJSON and indexed with a CosmosDB spatial index.
    /// For full AOT safety, use the overload accepting a string propertyName and Func delegate.
    /// </summary>
    [UnconditionalSuppressMessage("Trimming", "IL2075", Justification = "Property is resolved by name from a user-provided expression; the type is user-constructed and not subject to trimming.")]
    public CosmosDbDocumentStoreOptions MapSpatialProperty<T>(Expression<Func<T, GeoPoint>> property) where T : class
    {
        var body = property.Body;
        if (body is not MemberExpression member)
            throw new ArgumentException(
                "Expression must be a simple property access (e.g., x => x.Location).",
                nameof(property));

        var propertyName = member.Member.Name;
        var propInfo = typeof(T).GetProperty(propertyName)
            ?? throw new ArgumentException($"Property '{propertyName}' not found on type '{typeof(T).Name}'.");

        this.spatialMappings[typeof(T)] = new CosmosDbSpatialMapping
        {
            DocumentType = typeof(T),
            PropertyName = propertyName,
            GetGeoPoint = obj => (GeoPoint)propInfo.GetValue(obj)!
        };
        return this;
    }

    /// <summary>
    /// Declares that type T has a GeoPoint property to be used for spatial queries.
    /// AOT-safe overload that accepts a direct accessor delegate.
    /// </summary>
    public CosmosDbDocumentStoreOptions MapSpatialProperty<T>(string propertyName, Func<T, GeoPoint> accessor) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(propertyName);
        ArgumentNullException.ThrowIfNull(accessor);

        this.spatialMappings[typeof(T)] = new CosmosDbSpatialMapping
        {
            DocumentType = typeof(T),
            PropertyName = propertyName,
            GetGeoPoint = obj => accessor((T)obj)
        };
        return this;
    }

    internal CosmosDbSpatialMapping? ResolveSpatialMapping(Type type) =>
        this.spatialMappings.TryGetValue(type, out var mapping) ? mapping : null;

    internal void ResolveSpatialJsonPaths(JsonSerializerOptions jsonOptions)
    {
        foreach (var mapping in this.spatialMappings.Values)
        {
            if (mapping.JsonPath != null)
                continue;

            var jsonName = jsonOptions.PropertyNamingPolicy?.ConvertName(mapping.PropertyName) ?? mapping.PropertyName;
            mapping.JsonPath = jsonName;
        }
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

internal class CosmosDbSpatialMapping
{
    public required Type DocumentType { get; init; }
    public required string PropertyName { get; init; }
    public string? JsonPath { get; set; }
    public required Func<object, GeoPoint> GetGeoPoint { get; init; }
}
