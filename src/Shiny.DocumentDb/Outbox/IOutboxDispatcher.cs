namespace Shiny.DocumentDb;

/// <summary>
/// Delivers an outbox message to wherever it is going — a bus, an HTTP endpoint, an in-process mediator.
/// This is the seam the library deliberately does not fill: DocumentDb owns the transactional record and the
/// retry/dead-letter machinery, you own the transport.
/// </summary>
/// <remarks>
/// Resolved from a <b>fresh DI scope per message</b>, so scoped dependencies work normally. Register it with
/// <c>services.AddDocumentOutbox&lt;MyDispatcher&gt;()</c>, or skip the type entirely and pass a delegate to
/// <c>AddDocumentOutbox</c>.
/// </remarks>
public interface IOutboxDispatcher
{
    /// <summary>
    /// Delivers one message. Return normally to acknowledge it; throw to fail the attempt, which records the
    /// error, applies the backoff, and eventually dead-letters the message.
    /// </summary>
    Task Dispatch(OutboxMessage message, CancellationToken cancellationToken);
}

/// <summary>Adapts an <c>AddDocumentOutbox(dispatch)</c> delegate to <see cref="IOutboxDispatcher"/>.</summary>
sealed class DelegateOutboxDispatcher(Func<OutboxMessage, CancellationToken, Task> dispatch) : IOutboxDispatcher
{
    public Task Dispatch(OutboxMessage message, CancellationToken cancellationToken)
        => dispatch(message, cancellationToken);
}
