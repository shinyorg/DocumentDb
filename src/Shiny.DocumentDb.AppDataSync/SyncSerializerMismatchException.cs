namespace Shiny.DocumentDb.AppDataSync;


/// <summary>
/// Thrown at startup when a sync-registered type serializes differently through DocumentDb's
/// <c>JsonSerializerOptions</c> and Shiny.Data.Sync's <c>Shiny.Json.Default</c> serializer. The two
/// must produce the same wire format, or outbound JSON will not round-trip back through <c>Upsert</c>
/// and the backend sees an ambiguous contract. Align the naming policy / converters on both sides
/// (e.g. configure both for camelCase).
/// </summary>
public sealed class SyncSerializerMismatchException : InvalidOperationException
{
    public SyncSerializerMismatchException(Type documentType, string documentDbJson, string syncJson)
        : base(
            $"Sync-registered type '{documentType.FullName}' serializes differently through DocumentDb " +
            $"and Shiny.Data.Sync. They must share one JSON contract.{Environment.NewLine}" +
            $"  DocumentDb: {documentDbJson}{Environment.NewLine}" +
            $"  Shiny.Sync: {syncJson}{Environment.NewLine}" +
            "Align the naming policy and converters on both serializers."
        )
    {
        this.DocumentType = documentType;
    }

    public Type DocumentType { get; }
}
