using System.Text.Json.Serialization;

namespace Shiny.DocumentDb;

/// <summary>
/// A pending domain event — an ordinary document, so you can query it, back it up and replicate it with
/// everything else in the store.
/// </summary>
/// <remarks>
/// <para>
/// Every property carries an explicit <see cref="JsonPropertyNameAttribute"/>. That is part of this type's
/// contract, not a style choice: outbox rows are read by <b>other processes</b> — ShinyDocDbMyAdmin's outbox
/// screen addresses these fields by their stored names — so the wire shape must not follow whatever
/// <c>PropertyNamingPolicy</c> the owning application happens to configure. The names are mirrored by
/// <see cref="OutboxFields"/> and asserted by a guard test under a hostile naming policy.
/// </para>
/// <para>
/// Delivery is <b>at-least-once</b>. A dispatcher that succeeds and then crashes before its acknowledgement is
/// written will deliver the same message again, so consumers must be idempotent.
/// </para>
/// </remarks>
public sealed class OutboxMessage
{
    /// <summary>A monotonic version-7 (time-ordered) UUID in "N" format, so ordering by id is FIFO without a
    /// second index.</summary>
    [JsonPropertyName("id")]
    public string Id { get; set; } = null!;

    /// <summary>The logical event name a dispatcher switches on — the CLR type's name by default.</summary>
    [JsonPropertyName("messageType")]
    public required string MessageType { get; set; }

    /// <summary>The serialized event body.</summary>
    [JsonPropertyName("payload")]
    public required string Payload { get; set; }

    /// <summary>The ordering scope. Null means the message has no ordering requirement.</summary>
    [JsonPropertyName("partitionKey")]
    public string? PartitionKey { get; set; }

    /// <summary>Arbitrary transport metadata, including the <c>traceparent</c> captured at enqueue.</summary>
    [JsonPropertyName("headers")]
    public Dictionary<string, string>? Headers { get; set; }

    /// <summary>When the message was enqueued.</summary>
    [JsonPropertyName("createdAt")]
    public DateTimeOffset CreatedAt { get; set; }

    /// <summary>The backoff gate — the processor ignores the message until this moment passes.</summary>
    [JsonPropertyName("availableAt")]
    public DateTimeOffset AvailableAt { get; set; }

    /// <summary>How many times delivery has been attempted.</summary>
    [JsonPropertyName("attempts")]
    public int Attempts { get; set; }

    /// <summary>When the message was successfully delivered. Null while it is still in flight.</summary>
    [JsonPropertyName("processedAt")]
    public DateTimeOffset? ProcessedAt { get; set; }

    /// <summary>When the message exhausted its attempts. Dead letters are never deleted — requeue or purge them.</summary>
    [JsonPropertyName("deadLetteredAt")]
    public DateTimeOffset? DeadLetteredAt { get; set; }

    /// <summary>The last dispatch failure.</summary>
    [JsonPropertyName("error")]
    public string? Error { get; set; }

    /// <summary>
    /// The optimistic-concurrency counter — and the claim primitive. Two processors that read the same message
    /// both try to write it back; the loser gets a <see cref="ConcurrencyException"/> and moves on.
    /// </summary>
    [JsonPropertyName("version")]
    public int Version { get; set; }
}

/// <summary>
/// The pinned wire names of <see cref="OutboxMessage"/> — the stored JSON keys, which are <b>not</b> the CLR
/// property names. Anything addressing outbox rows without a CLR type (the admin tool's JSON-collection lane,
/// a hand-written query) uses these rather than <c>nameof</c>, which would produce <c>"ProcessedAt"</c> where
/// the stored key is <c>"processedAt"</c>.
/// </summary>
public static class OutboxFields
{
    public const string Id = "id";
    public const string MessageType = "messageType";
    public const string Payload = "payload";
    public const string PartitionKey = "partitionKey";
    public const string Headers = "headers";
    public const string CreatedAt = "createdAt";
    public const string AvailableAt = "availableAt";
    public const string Attempts = "attempts";
    public const string ProcessedAt = "processedAt";
    public const string DeadLetteredAt = "deadLetteredAt";
    public const string Error = "error";
    public const string Version = "version";

    /// <summary>Every pinned name, for the guard test and for schema-free readers that enumerate them.</summary>
    public static IReadOnlyList<string> All { get; } =
    [
        Id, MessageType, Payload, PartitionKey, Headers,
        CreatedAt, AvailableAt, Attempts, ProcessedAt, DeadLetteredAt, Error, Version
    ];

    /// <summary>The header key carrying the W3C trace context captured when the message was enqueued.</summary>
    public const string TraceParentHeader = "traceparent";
}
