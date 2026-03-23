using System.Linq.Expressions;
using System.Text.Json;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb;

public enum TypeNameResolution
{
    ShortName,
    FullName
}

public class DocumentStoreOptions
{
    readonly Dictionary<string, string> typeMappings = new();
    readonly HashSet<string> mappedTableNames = new(StringComparer.OrdinalIgnoreCase);
    readonly Dictionary<Type, string> idPropertyOverrides = new();

    public required IDatabaseProvider DatabaseProvider { get; set; }
    public TypeNameResolution TypeNameResolution { get; set; } = TypeNameResolution.ShortName;
    public JsonSerializerOptions? JsonSerializerOptions { get; set; }

    /// <summary>
    /// The name of the default shared document table.
    /// Types not explicitly mapped via <see cref="MapTypeToTable{T}"/> are stored here.
    /// Defaults to "documents".
    /// </summary>
    public string TableName { get; set; } = "documents";

    /// <summary>
    /// When false, calling a reflection-based overload (without JsonTypeInfo&lt;T&gt;) throws an
    /// InvalidOperationException if the type cannot be resolved from the configured TypeInfoResolver.
    /// Set to false in AOT deployments to get clear errors instead of hard-to-diagnose trimming failures.
    /// Defaults to true.
    /// </summary>
    public bool UseReflectionFallback { get; set; } = true;

    /// <summary>
    /// Optional callback invoked with every SQL statement the store executes.
    /// Useful for debugging and diagnostics.
    /// </summary>
    public Action<string>? Logging { get; set; }

    /// <summary>
    /// Maps a document type to its own dedicated table.
    /// The table name is auto-derived from the type name using the configured <see cref="TypeNameResolution"/>.
    /// </summary>
    public DocumentStoreOptions MapTypeToTable<T>() where T : class
    {
        var typeName = TypeNameResolver.Resolve(typeof(T), this.TypeNameResolution);
        return this.MapTypeToTable<T>(typeName);
    }

    /// <summary>
    /// Maps a document type to its own dedicated table with a custom Id property.
    /// The table name is auto-derived from the type name using the configured <see cref="TypeNameResolution"/>.
    /// </summary>
    public DocumentStoreOptions MapTypeToTable<T>(Expression<Func<T, object>> idProperty) where T : class
    {
        var typeName = TypeNameResolver.Resolve(typeof(T), this.TypeNameResolution);
        return this.MapTypeToTable<T>(typeName, idProperty);
    }

    /// <summary>
    /// Maps a document type to a dedicated table with the specified name.
    /// </summary>
    /// <exception cref="ArgumentException">Thrown if another type is already mapped to the same table name.</exception>
    public DocumentStoreOptions MapTypeToTable<T>(string tableName) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);
        var typeName = TypeNameResolver.Resolve(typeof(T), this.TypeNameResolution);

        if (!this.mappedTableNames.Add(tableName))
            throw new ArgumentException($"Table '{tableName}' is already mapped to another type.", nameof(tableName));

        this.typeMappings[typeName] = tableName;
        return this;
    }

    /// <summary>
    /// Maps a document type to a dedicated table with the specified name and a custom Id property.
    /// </summary>
    /// <exception cref="ArgumentException">Thrown if another type is already mapped to the same table name.</exception>
    public DocumentStoreOptions MapTypeToTable<T>(string tableName, Expression<Func<T, object>> idProperty) where T : class
    {
        this.MapTypeToTable<T>(tableName);
        this.idPropertyOverrides[typeof(T)] = ExtractPropertyName(idProperty);
        return this;
    }

    internal string ResolveTableName(string typeName)
        => this.typeMappings.TryGetValue(typeName, out var table) ? table : this.TableName;

    internal string? ResolveIdPropertyName(Type type)
        => this.idPropertyOverrides.TryGetValue(type, out var name) ? name : null;

    static string ExtractPropertyName<T>(Expression<Func<T, object>> expression)
    {
        var body = expression.Body;

        // Unwrap Convert (boxing value types to object)
        if (body is UnaryExpression { NodeType: ExpressionType.Convert } unary)
            body = unary.Operand;

        if (body is MemberExpression member)
            return member.Member.Name;

        throw new ArgumentException(
            "Expression must be a simple property access (e.g., x => x.MyId).",
            nameof(expression));
    }
}
