namespace Shiny.DocumentDb;

/// <summary>
/// A write was rejected because it would give two documents of the same type the same value for a unique
/// index declared with <c>MapUniqueIndex</c>. Nothing was written.
/// <para>
/// The duplicated value itself is deliberately not part of the message — unique keys are very often personal
/// data (an email address, a national id) and exception messages end up in logs.
/// </para>
/// </summary>
public sealed class UniqueConstraintException : InvalidOperationException
{
    public UniqueConstraintException(string typeName, UniqueIndexMapping index, string? documentId = null, Exception? innerException = null)
        : base(Format(typeName, index, documentId), innerException)
    {
        ArgumentNullException.ThrowIfNull(index);
        this.TypeName = typeName;
        this.DocumentType = index.DocumentType;
        this.IndexName = index.Name;
        this.PropertyNames = index.PropertyNames;
        this.DocumentId = documentId;
    }

    /// <summary>The stored type name of the rejected document.</summary>
    public string TypeName { get; }

    /// <summary>The CLR type the violated index is declared on.</summary>
    public Type DocumentType { get; }

    /// <summary>The violated index — <see cref="UniqueIndexMapping.Name"/>.</summary>
    public string IndexName { get; }

    /// <summary>The properties making up the violated key.</summary>
    public IReadOnlyList<string> PropertyNames { get; }

    /// <summary>The id of the rejected document, when the write that failed was for a single known document.</summary>
    public string? DocumentId { get; }

    static string Format(string typeName, UniqueIndexMapping index, string? documentId)
    {
        var key = index.PropertyNames.Count == 1
            ? index.PropertyNames[0]
            : "(" + string.Join(", ", index.PropertyNames) + ")";

        return documentId == null
            ? $"A document of type '{typeName}' with the same {key} already exists (unique index '{index.Name}')."
            : $"Document '{documentId}' of type '{typeName}' has the same {key} as an existing document (unique index '{index.Name}').";
    }
}
