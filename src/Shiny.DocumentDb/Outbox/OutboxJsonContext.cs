using System.Text.Json.Serialization;

namespace Shiny.DocumentDb;

/// <summary>
/// Source-generated metadata for <see cref="OutboxMessage"/>, so an AOT/trimmed application can run the outbox
/// without a reflection fallback: <c>o.MessageTypeInfo = OutboxJsonContext.Default.OutboxMessage;</c>.
/// </summary>
[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(OutboxMessage))]
public sealed partial class OutboxJsonContext : JsonSerializerContext;
