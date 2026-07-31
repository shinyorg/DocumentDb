using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb.Extensions.AI.Internal;

sealed class DocumentAIToolBuilder : IDocumentAIToolBuilder
{
    readonly Dictionary<string, DocumentAIRegistration> registrations = new(StringComparer.Ordinal);

    public IReadOnlyDictionary<string, DocumentAIRegistration> Registrations => this.registrations;

    public IDocumentAIToolBuilder AddType<T>(
        JsonTypeInfo<T> jsonTypeInfo,
        string? name = null,
        DocumentAICapabilities capabilities = DocumentAICapabilities.ReadOnly,
        Action<IDocumentAITypeBuilder<T>>? configure = null) where T : class
    {
        ArgumentNullException.ThrowIfNull(jsonTypeInfo);
        if (capabilities == DocumentAICapabilities.None)
            throw new ArgumentException("At least one capability must be specified.", nameof(capabilities));

        var slug = string.IsNullOrWhiteSpace(name) ? Slugify(typeof(T).Name) : name!;
        var typeBuilder = new DocumentAITypeBuilder<T>(jsonTypeInfo);
        configure?.Invoke(typeBuilder);

        this.Add(slug, typeBuilder.Build(slug, capabilities));
        return this;
    }

    public IDocumentAIToolBuilder AddCollection(
        string collectionName,
        string? name = null,
        DocumentAICapabilities capabilities = DocumentAICapabilities.ReadOnly,
        Action<IDocumentAICollectionBuilder>? configure = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(collectionName);
        if (capabilities == DocumentAICapabilities.None)
            throw new ArgumentException("At least one capability must be specified.", nameof(capabilities));

        var slug = string.IsNullOrWhiteSpace(name) ? Slugify(collectionName) : name!;
        var collectionBuilder = new DocumentAICollectionBuilder(collectionName);
        configure?.Invoke(collectionBuilder);

        this.Add(slug, collectionBuilder.Build(slug, capabilities));
        return this;
    }

    void Add(string slug, DocumentAIRegistration registration)
    {
        if (!this.registrations.TryAdd(slug, registration))
            throw new InvalidOperationException(
                $"A document type or collection with slug '{slug}' is already registered. Slugs must be unique.");
    }

    static string Slugify(string typeName)
    {
        // Camel/Pascal -> snake_case-ish, lowered. Customer -> customer, OrderLine -> order_line.
        var sb = new System.Text.StringBuilder(typeName.Length + 4);
        for (var i = 0; i < typeName.Length; i++)
        {
            var c = typeName[i];
            if (i > 0 && char.IsUpper(c) && !char.IsUpper(typeName[i - 1]))
                sb.Append('_');
            sb.Append(char.ToLowerInvariant(c));
        }
        return sb.ToString();
    }
}
