namespace Shiny.DocumentDb;

/// <summary>
/// Thrown when an optimistic concurrency check fails during an update.
/// The document was modified by another writer since it was last read.
/// </summary>
public class ConcurrencyException : Exception
{
    public string TypeName { get; }
    public string DocumentId { get; }
    public int ExpectedVersion { get; }
    public int? ActualVersion { get; }

    public ConcurrencyException(string typeName, string documentId, int expectedVersion, int? actualVersion = null)
        : base(BuildMessage(typeName, documentId, expectedVersion, actualVersion))
    {
        TypeName = typeName;
        DocumentId = documentId;
        ExpectedVersion = expectedVersion;
        ActualVersion = actualVersion;
    }

    static string BuildMessage(string typeName, string documentId, int expectedVersion, int? actualVersion)
    {
        var msg = $"Concurrency conflict on document '{typeName}' with Id '{documentId}'. Expected version {expectedVersion}";
        if (actualVersion.HasValue)
            msg += $", but found version {actualVersion.Value}";
        msg += ". The document was modified by another writer.";
        return msg;
    }
}
