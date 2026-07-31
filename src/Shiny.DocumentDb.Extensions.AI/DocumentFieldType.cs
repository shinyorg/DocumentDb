namespace Shiny.DocumentDb.Extensions.AI;

/// <summary>
/// The type of a field declared on a schema-free JSON collection. A CLR document type carries this in its
/// metadata; a collection addressed by name does not, so it is declared here — see
/// <see cref="IDocumentAICollectionBuilder.Field"/>.
/// </summary>
/// <remarks>
/// It does two jobs: it becomes the field's type in the JSON schema the LLM sees, and it becomes the
/// <c>:type</c> hint on the generated string-grammar filter — without which a numeric sort or comparison
/// falls back to text ordering on every provider except SQLite.
/// </remarks>
public enum DocumentFieldType
{
    String,
    Number,
    Int,
    Long,
    Double,
    Decimal,
    Bool,
    Date,
    Guid
}
