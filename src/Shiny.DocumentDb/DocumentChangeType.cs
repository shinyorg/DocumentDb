namespace Shiny.DocumentDb;

/// <summary>
/// Describes the kind of change that occurred to a document.
/// </summary>
public enum DocumentChangeType
{
    /// <summary>A new document was inserted (also raised for each row of a batch insert).</summary>
    Inserted,

    /// <summary>
    /// An existing document was modified. Raised for <c>Update</c>, <c>Upsert</c>,
    /// <c>SetProperty</c> and <c>RemoveProperty</c>.
    /// </summary>
    Updated,

    /// <summary>A single document was removed.</summary>
    Removed,

    /// <summary>All documents of the type were cleared in a single operation.</summary>
    Cleared
}
