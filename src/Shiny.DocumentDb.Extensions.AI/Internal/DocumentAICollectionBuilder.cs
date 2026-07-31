namespace Shiny.DocumentDb.Extensions.AI.Internal;

sealed class DocumentAICollectionBuilder : IDocumentAICollectionBuilder
{
    readonly string collectionName;
    string? description;
    string idProperty = "id";
    List<JsonCollectionField>? fields;
    bool allowAnyField;
    List<string>? filters;
    int maxPageSize = 100;

    public DocumentAICollectionBuilder(string collectionName)
    {
        this.collectionName = collectionName;
    }

    public IDocumentAICollectionBuilder Description(string description)
    {
        this.description = description;
        return this;
    }

    public IDocumentAICollectionBuilder IdProperty(string idProperty)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(idProperty);
        this.idProperty = PathGuard.Validate(idProperty);
        return this;
    }

    public IDocumentAICollectionBuilder Field(string path, DocumentFieldType type, string? description = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        PathGuard.Validate(path);

        this.fields ??= new List<JsonCollectionField>();
        if (this.fields.Any(f => string.Equals(f.Path, path, StringComparison.Ordinal)))
            throw new InvalidOperationException($"Field '{path}' is already declared on collection '{this.collectionName}'.");

        this.fields.Add(new JsonCollectionField(path, type, description));
        return this;
    }

    public IDocumentAICollectionBuilder AllowAnyField()
    {
        this.allowAnyField = true;
        return this;
    }

    public IDocumentAICollectionBuilder MaxPageSize(int maxPageSize)
    {
        if (maxPageSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(maxPageSize));
        this.maxPageSize = maxPageSize;
        return this;
    }

    public IDocumentAICollectionBuilder Where(string filter)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(filter);
        this.filters ??= new List<string>();
        this.filters.Add(filter);
        return this;
    }

    public DocumentAICollectionRegistration Build(string slug, DocumentAICapabilities caps)
    {
        if (this.allowAnyField && this.fields != null)
            throw new InvalidOperationException(
                $"Collection '{this.collectionName}' calls both Field(...) and AllowAnyField(). " +
                "Declare the fields the LLM may use, or open the collection up — not both.");

        if (!this.allowAnyField && this.fields is null)
            throw new InvalidOperationException(
                $"Collection '{this.collectionName}' declares no fields. A schema-free collection has no " +
                "metadata to discover them from, so declare each field with Field(path, type) — or call " +
                "AllowAnyField() to let the LLM name any path.");

        // The scope filter is a hard boundary on a typed registration because the predicate can be evaluated
        // against the materialized document before a write. A raw JSON body has no such evaluator, so rather
        // than let an insert slip outside the scope, the combination is refused.
        if (this.filters is { Count: > 0 } &&
            (caps.HasFlag(DocumentAICapabilities.Insert) || caps.HasFlag(DocumentAICapabilities.Update)))
        {
            throw new InvalidOperationException(
                $"Collection '{this.collectionName}' combines Where(...) with Insert/Update. A schema-free " +
                "collection can only enforce the filter on reads, so the write capabilities would escape it. " +
                "Drop the write capabilities, or register the type with AddType<T>() where the filter is " +
                "enforced on both sides of a write.");
        }

        return new DocumentAICollectionRegistration
        {
            Slug = slug,
            Capabilities = caps,
            CollectionName = this.collectionName,
            IdProperty = this.idProperty,
            Description = this.description,
            Fields = this.fields,
            Filters = (IReadOnlyList<string>?)this.filters ?? Array.Empty<string>(),
            MaxPageSize = this.maxPageSize
        };
    }
}
