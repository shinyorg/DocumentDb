using System.Diagnostics.CodeAnalysis;
using System.Text.Json;

namespace Shiny.DocumentDb;

/// <summary>
/// The declarative half of enqueue: "every write of this type publishes an event". Built on the public
/// interceptor surface, so it adds no member to any options class and works on every provider.
/// </summary>
public static class PublishToOutboxExtensions
{
    /// <summary>
    /// Publishes an outbox message for each write of <typeparamref name="T"/>. The message is inserted in the
    /// <b>same transaction</b> as the write that triggered it, from
    /// <see cref="IDocumentInterceptor.AfterWrite"/> — so the event exists exactly when the write commits.
    /// <code>
    /// options.ConfigureDocument&lt;Order&gt;(cfg =>
    ///     cfg.PublishToOutbox(ctx => new OrderChanged(ctx.Id!.ToString()!, ctx.Operation)));
    /// </code>
    /// </summary>
    /// <param name="eventFactory">
    /// Builds the event from the write. Return <c>null</c> to publish nothing for that write — the hook for
    /// "only when the status actually changed". <c>ctx.Document</c> is null for a delete-by-id, where only
    /// <c>ctx.Id</c> is available.
    /// </param>
    /// <param name="operations">Which write kinds publish. Defaults to all of them.</param>
    /// <param name="partitionKey">
    /// The ordering scope for the message. Return the aggregate id to make every event for one aggregate
    /// strictly ordered (with <c>OutboxOptions.OrderedPartitions</c> on).
    /// </param>
    [RequiresUnreferencedCode("Serializes the event object with reflection. Enqueue explicitly through ctx.Session in a trimmed or AOT application.")]
    [RequiresDynamicCode("Serializes the event object with reflection. Enqueue explicitly through ctx.Session in a trimmed or AOT application.")]
    public static DocumentTypeBuilder<T> PublishToOutbox<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(
        this DocumentTypeBuilder<T> cfg,
        Func<DocumentWriteContext, object?> eventFactory,
        OutboxOperations operations = OutboxOperations.All,
        Func<DocumentWriteContext, string?>? partitionKey = null) where T : class
    {
        ArgumentNullException.ThrowIfNull(cfg);
        ArgumentNullException.ThrowIfNull(eventFactory);

        return cfg.OnAfterWrite(async (ctx, ct) =>
        {
            if (!Includes(operations, ctx.Operation))
                return;

            var evt = eventFactory(ctx);
            if (evt == null)
                return;

            var options = ctx.Services.GetService(typeof(OutboxOptions)) as OutboxOptions ?? OutboxDefaults.Options;
            var payload = JsonSerializer.Serialize(evt, evt.GetType(), options.PayloadSerializerOptions);

            // ctx.Session is bound to this write's transaction, so the flush joins it rather than committing
            // separately. Interceptors are suppressed for the flush so the outbox insert cannot recurse back
            // into this hook (or into anyone else's).
            ctx.Session.EnqueueRaw(
                OutboxSessionExtensions.MessageTypeOf(evt.GetType()),
                payload,
                partitionKey?.Invoke(ctx));

            await ctx.Session.SaveChanges(suppressInterceptors: true, ct).ConfigureAwait(false);
        });
    }

    static bool Includes(OutboxOperations operations, DocumentOperation operation) => operation switch
    {
        DocumentOperation.Insert => operations.HasFlag(OutboxOperations.Insert),
        DocumentOperation.Update => operations.HasFlag(OutboxOperations.Update),
        DocumentOperation.Upsert => operations.HasFlag(OutboxOperations.Upsert),
        DocumentOperation.Delete => operations.HasFlag(OutboxOperations.Delete),
        // Clear is set-based: it never materializes the documents, so there is nothing to build an event from.
        _ => false
    };
}
