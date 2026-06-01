using Microsoft.Extensions.AI;

namespace Shiny.DocumentDb.Extensions.AI;

/// <summary>
/// Wires <see cref="IEmbeddingGenerator{TInput, TEmbedding}"/> into the document store's
/// <c>OnBeforeInsert</c> pipeline so a text property is automatically embedded into a
/// <see cref="ReadOnlyMemory{T}"/> field on insert / upsert / batch insert.
/// </summary>
public static class AutoEmbedExtensions
{
    /// <summary>
    /// Auto-populates <paramref name="targetSelector"/> from the embedding of
    /// <paramref name="sourceSelector"/> using the supplied <paramref name="generator"/>.
    /// Skips when the source is null/empty or the target already has a non-default value.
    /// </summary>
    public static DocumentStoreOptions AutoEmbedOnInsert<T>(
        this DocumentStoreOptions options,
        IEmbeddingGenerator<string, Embedding<float>> generator,
        Func<T, string?> sourceSelector,
        Action<T, ReadOnlyMemory<float>> targetSetter,
        Func<T, ReadOnlyMemory<float>>? targetGetter = null) where T : class
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(generator);
        ArgumentNullException.ThrowIfNull(sourceSelector);
        ArgumentNullException.ThrowIfNull(targetSetter);

        options.OnBeforeInsert<T>(async (doc, ct) =>
        {
            // Skip if a vector is already attached — caller may want to override the generator
            // by setting the field explicitly.
            if (targetGetter != null && targetGetter(doc).Length > 0)
                return;

            var text = sourceSelector(doc);
            if (string.IsNullOrEmpty(text))
                return;

            var embedding = await generator.GenerateAsync(text, cancellationToken: ct).ConfigureAwait(false);
            targetSetter(doc, embedding.Vector);
        });

        return options;
    }
}
