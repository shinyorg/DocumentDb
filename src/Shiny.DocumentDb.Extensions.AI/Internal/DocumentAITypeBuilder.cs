using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb.Extensions.AI.Internal;

sealed class DocumentAITypeBuilder<T> : IDocumentAITypeBuilder<T> where T : class
{
    readonly JsonTypeInfo<T> typeInfo;
    string? typeDescription;
    readonly Dictionary<string, string> propertyDescriptions = new(StringComparer.Ordinal);
    List<string>? allowed;
    List<string>? ignored;
    int maxPageSize = 100;

    public DocumentAITypeBuilder(JsonTypeInfo<T> typeInfo)
    {
        this.typeInfo = typeInfo;
    }

    public IDocumentAITypeBuilder<T> Description(string description)
    {
        this.typeDescription = description;
        return this;
    }

    public IDocumentAITypeBuilder<T> Property<TProp>(Expression<Func<T, TProp>> property, string description)
    {
        var jsonName = ResolveJsonName(property);
        this.propertyDescriptions[jsonName] = description;
        return this;
    }

    public IDocumentAITypeBuilder<T> AllowProperties(params Expression<Func<T, object?>>[] properties)
    {
        this.allowed ??= new List<string>();
        foreach (var p in properties)
            this.allowed.Add(ResolveJsonName(p));
        return this;
    }

    public IDocumentAITypeBuilder<T> IgnoreProperties(params Expression<Func<T, object?>>[] properties)
    {
        this.ignored ??= new List<string>();
        foreach (var p in properties)
            this.ignored.Add(ResolveJsonName(p));
        return this;
    }

    public IDocumentAITypeBuilder<T> MaxPageSize(int maxPageSize)
    {
        if (maxPageSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(maxPageSize));
        this.maxPageSize = maxPageSize;
        return this;
    }

    public DocumentAITypeRegistration<T> Build(string slug, DocumentAICapabilities caps) => new()
    {
        Slug = slug,
        Capabilities = caps,
        DocumentType = typeof(T),
        JsonTypeInfo = this.typeInfo,
        TypeDescriptionOverride = this.typeDescription,
        PropertyDescriptionOverrides = this.propertyDescriptions,
        AllowedProperties = this.allowed,
        IgnoredProperties = this.ignored,
        MaxPageSize = this.maxPageSize
    };

    string ResolveJsonName(LambdaExpression expression)
    {
        var clrName = ExtractMemberName(expression);
        // map CLR name -> JSON name through JsonTypeInfo (respects [JsonPropertyName] + naming policy)
        foreach (var prop in this.typeInfo.Properties)
        {
            if (prop.AttributeProvider is MemberInfo mi && mi.Name == clrName)
                return prop.Name;
        }
        return clrName;
    }

    static string ExtractMemberName(LambdaExpression expression)
    {
        var body = expression.Body;
        if (body is UnaryExpression { NodeType: ExpressionType.Convert } unary)
            body = unary.Operand;
        if (body is MemberExpression member)
            return member.Member.Name;
        throw new ArgumentException(
            "Property expression must be a simple member access (e.g. x => x.PropertyName).",
            nameof(expression));
    }
}
