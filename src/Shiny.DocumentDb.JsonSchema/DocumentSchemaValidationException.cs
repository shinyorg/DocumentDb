namespace Shiny.DocumentDb;

/// <summary>A single JSON Schema validation failure, flattened from the evaluation result.</summary>
/// <param name="InstanceLocation">JSON Pointer to the offending value in the document (e.g. <c>/email</c>).</param>
/// <param name="Keyword">The schema keyword that failed (e.g. <c>required</c>, <c>maxLength</c>, <c>format</c>).</param>
/// <param name="Message">The human-readable error message from the evaluator.</param>
public sealed record SchemaValidationError(string InstanceLocation, string Keyword, string Message);

/// <summary>
/// Thrown by the <c>Shiny.DocumentDb.JsonSchema</c> interceptor when a document fails validation against
/// its mapped JSON Schema. Throwing here aborts the write and rolls back the surrounding unit of work.
/// </summary>
public sealed class DocumentSchemaValidationException : Exception
{
    public DocumentSchemaValidationException(Type documentType, string typeName, IReadOnlyList<SchemaValidationError> errors)
        : base(BuildMessage(documentType, errors))
    {
        this.DocumentType = documentType;
        this.TypeName = typeName;
        this.Errors = errors;
    }

    /// <summary>The CLR type of the document that failed validation.</summary>
    public Type DocumentType { get; }

    /// <summary>The store type name of the document that failed validation.</summary>
    public string TypeName { get; }

    /// <summary>The flattened list of field-level validation failures.</summary>
    public IReadOnlyList<SchemaValidationError> Errors { get; }

    static string BuildMessage(Type documentType, IReadOnlyList<SchemaValidationError> errors)
    {
        var detail = errors.Count == 0
            ? "the document did not match its schema"
            : string.Join("; ", errors.Select(e =>
                $"{(string.IsNullOrEmpty(e.InstanceLocation) ? "(root)" : e.InstanceLocation)}: {e.Message} [{e.Keyword}]"));
        return $"Document of type '{documentType.Name}' failed JSON Schema validation: {detail}";
    }
}
