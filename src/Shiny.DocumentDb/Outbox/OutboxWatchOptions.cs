using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb;

/// <summary>Which bucket an <see cref="OutboxMessage"/> is in, derived from its three nullable timestamps.</summary>
[Flags]
public enum OutboxStates
{
    /// <summary>Undelivered and eligible now.</summary>
    Pending = 1,

    /// <summary>Undelivered, waiting out a backoff — <c>AvailableAt</c> is still in the future.</summary>
    Scheduled = 2,

    /// <summary>Out of attempts. Never deleted automatically; requeue or purge it.</summary>
    DeadLettered = 4,

    /// <summary>Delivered and acknowledged.</summary>
    Processed = 8,

    All = Pending | Scheduled | DeadLettered | Processed
}

/// <summary>Shapes the read-only stream returned by <c>store.WatchOutbox(…)</c>.</summary>
public sealed class OutboxWatchOptions
{
    /// <summary>Which states to yield. <see cref="OutboxStates.Pending"/> is the interesting one;
    /// <see cref="OutboxStates.DeadLettered"/> is the one worth alerting on.</summary>
    public OutboxStates States { get; set; } = OutboxStates.Pending;

    /// <summary>Restricts the stream to one ordering scope.</summary>
    public string? PartitionKey { get; set; }

    /// <summary>Restricts the stream to one message type.</summary>
    public string? MessageType { get; set; }

    /// <summary>Floor on how often the table is read. Defaults to 5 seconds.</summary>
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Yields what is already queued before tailing. On by default — a dashboard that starts mid-backlog and
    /// shows nothing is a bug, not a feature.
    /// </summary>
    public bool IncludeExisting { get; set; } = true;

    /// <summary>How many rows one poll reads. Defaults to 100.</summary>
    public int BatchSize { get; set; } = 100;

    /// <summary>The clock that decides Pending from Scheduled. Defaults to the system clock.</summary>
    public TimeProvider TimeProvider { get; set; } = TimeProvider.System;

    /// <summary>AOT-safe metadata for <see cref="OutboxMessage"/>. See <c>OutboxOptions.MessageTypeInfo</c>.</summary>
    public JsonTypeInfo<OutboxMessage>? MessageTypeInfo { get; set; }

    /// <summary>Serializer options for the payload, used by the decoding <c>WatchOutbox&lt;TEvent&gt;</c> overload.</summary>
    public JsonSerializerOptions PayloadSerializerOptions { get; set; } = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };
}

/// <summary>A watched message with its payload already deserialized.</summary>
public sealed record OutboxEnvelope<TEvent>(OutboxMessage Message, TEvent Event) where TEvent : class;
