using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb;

/// <summary>Operational helpers for a store with encrypted properties.</summary>
public static class EncryptionMaintenanceExtensions
{
    /// <summary>
    /// Re-encrypts every document of <typeparamref name="T"/> under the encryptor's <b>current</b> key, and converts
    /// any values written before the property was mapped. Reading decrypts with whichever key the envelope names and
    /// writing re-encrypts with the current one, so a plain round trip is the rewrap — this method is that round
    /// trip, batched.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The rotation sequence is: add the new key to the ring alongside the old one → make it
    /// <see cref="IDocumentEncryptor.CurrentKeyId"/> → run this → only then retire the old key. Retiring it earlier
    /// makes any document still holding an old envelope unreadable.
    /// </para>
    /// <para>
    /// Documents are read a page at a time and written back with <c>BatchUpdate</c>. It is a maintenance operation,
    /// not a hot path: run it from a job or a one-off tool, and expect it to touch every document of the type.
    /// Interceptors fire as they would for any update — suppress them around the call if that is not wanted.
    /// </para>
    /// </remarks>
    /// <param name="store">The store to rewrap in.</param>
    /// <param name="batchSize">Documents read and written per batch.</param>
    /// <param name="jsonTypeInfo">Optional AOT-safe metadata for <typeparamref name="T"/>.</param>
    /// <param name="progress">Reports the running total of rewrapped documents.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The number of documents rewritten.</returns>
    public static async Task<int> RewrapAsync<T>(
        this IDocumentStore store,
        int batchSize = 500,
        JsonTypeInfo<T>? jsonTypeInfo = null,
        IProgress<int>? progress = null,
        CancellationToken cancellationToken = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(batchSize);

        var total = 0;
        var offset = 0;
        while (true)
        {
            // Rewrapping changes neither membership nor ordering, so a plain offset walk is stable here.
            var batch = await store
                .Query<T>(jsonTypeInfo)
                .Paginate(offset, batchSize)
                .ToList(cancellationToken)
                .ConfigureAwait(false);

            if (batch.Count == 0)
                return total;

            await store.BatchUpdate(batch, jsonTypeInfo, cancellationToken).ConfigureAwait(false);

            total += batch.Count;
            offset += batch.Count;
            progress?.Report(total);
        }
    }
}
