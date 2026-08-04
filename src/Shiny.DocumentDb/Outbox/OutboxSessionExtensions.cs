using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb;

/// <summary>
/// The transactional half of the outbox: enqueue rides the caller's unit of work, so the event and the write
/// that made it happen commit together or not at all.
/// </summary>
public static class OutboxSessionExtensions
{
    /// <summary>
    /// Buffers a domain event as an <see cref="OutboxMessage"/> in this session. Nothing is written until
    /// <see cref="IDocumentSession.SaveChanges(CancellationToken)"/>, which flushes the message in the <b>same
    /// transaction</b> as everything else buffered — that atomicity is the entire point, so never reach for
    /// <c>store.Insert</c> in a separate call.
    /// <code>
    /// await using var session = store.OpenSession();
    /// session.Add(order).Enqueue(new OrderPlaced(order.Id, order.Total));
    /// await session.SaveChanges();
    /// </code>
    /// </summary>
    /// <param name="evt">The event. Serialized into <see cref="OutboxMessage.Payload"/>.</param>
    /// <param name="partitionKey">The ordering scope, honoured when <c>OutboxOptions.OrderedPartitions</c> is on.</param>
    /// <param name="headers">Extra transport metadata. <c>traceparent</c> is added automatically.</param>
    [RequiresUnreferencedCode("Serializes the event with reflection. Pass a JsonTypeInfo<TEvent> overload in a trimmed or AOT application.")]
    [RequiresDynamicCode("Serializes the event with reflection. Pass a JsonTypeInfo<TEvent> overload in a trimmed or AOT application.")]
    public static IDocumentSession Enqueue<TEvent>(
        this IDocumentSession session,
        TEvent evt,
        string? partitionKey = null,
        IDictionary<string, string>? headers = null) where TEvent : class
    {
        ArgumentNullException.ThrowIfNull(session);
        ArgumentNullException.ThrowIfNull(evt);

        var options = Resolve(session);
        var payload = JsonSerializer.Serialize(evt, options.PayloadSerializerOptions);
        return session.EnqueueRaw(MessageTypeOf(typeof(TEvent)), payload, partitionKey, headers);
    }

    /// <summary>AOT-safe overload — serializes the event through supplied metadata rather than reflection.</summary>
    public static IDocumentSession Enqueue<TEvent>(
        this IDocumentSession session,
        TEvent evt,
        JsonTypeInfo<TEvent> jsonTypeInfo,
        string? partitionKey = null,
        IDictionary<string, string>? headers = null) where TEvent : class
    {
        ArgumentNullException.ThrowIfNull(session);
        ArgumentNullException.ThrowIfNull(evt);
        ArgumentNullException.ThrowIfNull(jsonTypeInfo);

        var payload = JsonSerializer.Serialize(evt, jsonTypeInfo);
        return session.EnqueueRaw(MessageTypeOf(typeof(TEvent)), payload, partitionKey, headers);
    }

    /// <summary>
    /// Enqueues a pre-serialized event. The lane for a payload you already hold as JSON, or a message type that
    /// is a contract name rather than a CLR type. Spelled differently from <c>Enqueue</c> on purpose:
    /// <c>Enqueue("OrderPlaced", json)</c> would otherwise be a legal — and silently wrong — call to the
    /// generic overload with <c>TEvent</c> inferred as <see cref="string"/>.
    /// </summary>
    public static IDocumentSession EnqueueRaw(
        this IDocumentSession session,
        string messageType,
        string payload,
        string? partitionKey = null,
        IDictionary<string, string>? headers = null)
    {
        ArgumentNullException.ThrowIfNull(session);
        ArgumentException.ThrowIfNullOrWhiteSpace(messageType);
        ArgumentNullException.ThrowIfNull(payload);

        var options = Resolve(session);
        var clock = session.Services.GetService(typeof(TimeProvider)) as TimeProvider ?? TimeProvider.System;
        var now = clock.GetUtcNow();

        var message = new OutboxMessage
        {
            // Time-ordered and monotonic within the process, so the processor reads FIFO with OrderBy(Id) and
            // needs no second index. See OutboxId for why Guid.CreateVersion7 is not enough on its own.
            Id = OutboxId.Next(),
            MessageType = messageType,
            Payload = payload,
            PartitionKey = partitionKey,
            Headers = BuildHeaders(headers),
            CreatedAt = now,
            AvailableAt = now
        };
        return session.Add(message, options.MessageTypeInfo);
    }

    /// <summary>The logical message type recorded for a CLR event type.</summary>
    internal static string MessageTypeOf(Type type) => type.FullName ?? type.Name;

    // The event's causing request, so the consumer's span links back to it. Captured here rather than at
    // dispatch because by then the originating activity is long gone.
    static Dictionary<string, string>? BuildHeaders(IDictionary<string, string>? supplied)
    {
        var traceParent = Activity.Current?.Id;
        if (supplied == null && traceParent == null)
            return null;

        var headers = supplied == null
            ? new Dictionary<string, string>(StringComparer.Ordinal)
            : new Dictionary<string, string>(supplied, StringComparer.Ordinal);

        if (traceParent != null && !headers.ContainsKey(OutboxFields.TraceParentHeader))
            headers[OutboxFields.TraceParentHeader] = traceParent;

        return headers;
    }

    // The session's DI scope carries the registered OutboxOptions. A session opened outside DI (tests, MAUI)
    // has none, and the defaults are correct there — enqueue does not depend on any of the processor's knobs.
    static OutboxOptions Resolve(IDocumentSession session)
        => session.Services.GetService(typeof(OutboxOptions)) as OutboxOptions ?? OutboxDefaults.Options;
}

/// <summary>Fallbacks for the paths that run without a container.</summary>
static class OutboxDefaults
{
    public static OutboxOptions Options { get; } = new();
}
