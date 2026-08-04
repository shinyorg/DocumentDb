using System.Text.Json.Nodes;

namespace ShinyDocDbMyAdmin.Models;

/// <summary>Which bucket an outbox message is in, derived from its three nullable timestamps.</summary>
public enum OutboxState
{
    /// <summary>Undelivered and eligible now.</summary>
    Pending,

    /// <summary>Undelivered, waiting out a retry backoff.</summary>
    Scheduled,

    /// <summary>Out of attempts. Never removed automatically.</summary>
    DeadLettered,

    /// <summary>Delivered and acknowledged.</summary>
    Processed
}

/// <summary>Where an outbox lives in this database.</summary>
/// <param name="Table">The documents table holding the messages.</param>
/// <param name="TypeName">The <c>TypeName</c> value as stored — short or dotted, depending on the
/// application's <c>TypeNameResolution</c>.</param>
/// <param name="Count">How many messages the table holds, in any state.</param>
/// <param name="Addressable">
/// False when <paramref name="TypeName"/> is dotted. Collection names are validated to
/// <c>^[A-Za-z_][A-Za-z0-9_]{0,127}$</c> because they are interpolated into DDL, so a dotted name can be
/// browsed but not queried through the JSON-collection lane — the screen says so and points at the SQL console
/// instead of failing.
/// </param>
public sealed record OutboxLocation(string Table, string TypeName, long Count, bool Addressable);

/// <summary>One outbox row, flattened out of the JSON body for the grid.</summary>
public sealed record OutboxEntry(
    string Id,
    string MessageType,
    string? PartitionKey,
    DateTimeOffset? CreatedAt,
    DateTimeOffset? AvailableAt,
    int Attempts,
    OutboxState State,
    string? Error,
    string? TraceParent,
    JsonNode? Payload)
{
    /// <summary>The first line of <see cref="Error"/>, for a grid cell that must not wrap.</summary>
    public string? ErrorSummary => OutboxErrors.FirstLine(this.Error);
}

/// <summary>
/// The health strip. <see cref="OldestPendingAt"/> is the number that matters: a healthy busy system has a
/// large pending count, while a system whose processor died has an <i>old</i> one. Depth alone cannot tell
/// those apart.
/// </summary>
public sealed record OutboxHealth(
    long Pending,
    long Scheduled,
    long DeadLettered,
    long Processed,
    DateTimeOffset? OldestPendingAt)
{
    public long Total => this.Pending + this.Scheduled + this.DeadLettered + this.Processed;

    /// <summary>How long the oldest eligible message has been waiting, or null when nothing is pending.</summary>
    public TimeSpan? OldestPendingAge(DateTimeOffset now)
        => this.OldestPendingAt is { } at ? now - at : null;
}

/// <summary>The grid's filter. All parts are optional and AND together.</summary>
public sealed record OutboxFilter(
    OutboxState? State = null,
    string? MessageType = null,
    string? PartitionKey = null,
    int Offset = 0,
    int Take = 100);

/// <summary>Dead letters collapsed by message type and the first line of the error.</summary>
public sealed record OutboxFailureGroup(string MessageType, string ErrorSummary, int Count, string SampleId);

/// <summary>The result of a grouped failure read, including whether the row cap bit.</summary>
public sealed record OutboxFailureReport(IReadOnlyList<OutboxFailureGroup> Groups, int Scanned, bool Capped);

/// <summary>Normalizes dispatch errors so two instances of the same failure collapse into one row.</summary>
public static partial class OutboxErrors
{
    /// <summary>The first line of an error, for a grid cell that must not wrap.</summary>
    public static string? FirstLine(string? error)
    {
        if (string.IsNullOrWhiteSpace(error))
            return null;

        var end = error.AsSpan().IndexOfAny('\r', '\n');
        return (end < 0 ? error : error[..end]).Trim();
    }

    /// <summary>
    /// The first line with embedded identifiers replaced by a placeholder, so "Order 4f2a… not found" and
    /// "Order 9c11… not found" collapse into one failure group rather than producing a row each. Deliberately
    /// narrow — GUIDs and long numbers only — because over-eager normalization would merge genuinely different
    /// failures, which is worse than showing two rows.
    /// </summary>
    public static string Summarize(string? error)
        => FirstLine(error) is { } line ? Identifiers().Replace(line, "…").Trim() : "(no error recorded)";

    [System.Text.RegularExpressions.GeneratedRegex(
        @"\b[0-9a-fA-F]{8}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{12}\b|\b\d{3,}\b")]
    private static partial System.Text.RegularExpressions.Regex Identifiers();
}
