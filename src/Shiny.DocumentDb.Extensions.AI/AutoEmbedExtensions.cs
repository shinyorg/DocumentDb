using Microsoft.Extensions.AI;
using Microsoft.Extensions.DependencyInjection;

namespace Shiny.DocumentDb.Extensions.AI;

/// <summary>
/// Wires <see cref="IEmbeddingGenerator{TInput, TEmbedding}"/> into the document store's write pipeline as an
/// <see cref="IDocumentInterceptor"/> so a text property is automatically embedded into a
/// <see cref="ReadOnlyMemory{T}"/> field on insert / upsert / batch insert. Because it rides the interceptor
/// pipeline it fires on <b>every</b> provider (relational and document-native — Cosmos, MongoDB, Redis, …),
/// inside the write's transaction, and — via the DI overload — resolves the generator <b>per write from the
/// caller's scope</b>, so per-tenant / per-scope model selection works.
/// </summary>
public static class AutoEmbedExtensions
{
    /// <summary>
    /// Auto-populates <paramref name="targetSetter"/> from the embedding of <paramref name="sourceSelector"/>,
    /// resolving the <see cref="IEmbeddingGenerator{TInput, TEmbedding}"/> from the write's DI scope
    /// (<c>ctx.Services</c>) — register one in the container. Skips when the source is null/empty or the target
    /// already has a non-default value. Use the <see cref="AutoEmbedOnInsert{T}(DocumentStoreOptions, IEmbeddingGenerator{string, Embedding{float}}, Func{T, string}, Action{T, ReadOnlyMemory{float}}, Func{T, ReadOnlyMemory{float}})"/>
    /// overload for the container-free path (<c>new DocumentStore(options)</c>) or a fixed generator instance.
    /// </summary>
    public static DocumentStoreOptions AutoEmbedOnInsert<T>(
        this DocumentStoreOptions options,
        Func<T, string?> sourceSelector,
        Action<T, ReadOnlyMemory<float>> targetSetter,
        Func<T, ReadOnlyMemory<float>>? targetGetter = null) where T : class
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(sourceSelector);
        ArgumentNullException.ThrowIfNull(targetSetter);

        options.AddInterceptor(new AutoEmbedInterceptor<T>(null, sourceSelector, targetSetter, targetGetter));
        return options;
    }

    /// <summary>
    /// Auto-populates <paramref name="targetSetter"/> from the embedding of <paramref name="sourceSelector"/>
    /// using the supplied <paramref name="generator"/> (a fixed instance — the right choice for the container-free
    /// <c>new DocumentStore(options)</c> path). Skips when the source is null/empty or the target already has a
    /// non-default value.
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

        options.AddInterceptor(new AutoEmbedInterceptor<T>(generator, sourceSelector, targetSetter, targetGetter));
        return options;
    }
}

/// <summary>
/// Per-document interceptor that generates an embedding on insert / upsert. Holds an optional fixed generator;
/// when null it resolves <see cref="IEmbeddingGenerator{TInput, TEmbedding}"/> fresh from the write's scope
/// (<see cref="DocumentWriteContext.Services"/>) each time, so a scoped session yields the caller's own generator.
/// </summary>
sealed class AutoEmbedInterceptor<T> : IDocumentInterceptor where T : class
{
    readonly IEmbeddingGenerator<string, Embedding<float>>? generator;
    readonly Func<T, string?> sourceSelector;
    readonly Action<T, ReadOnlyMemory<float>> targetSetter;
    readonly Func<T, ReadOnlyMemory<float>>? targetGetter;

    public AutoEmbedInterceptor(
        IEmbeddingGenerator<string, Embedding<float>>? generator,
        Func<T, string?> sourceSelector,
        Action<T, ReadOnlyMemory<float>> targetSetter,
        Func<T, ReadOnlyMemory<float>>? targetGetter)
    {
        this.generator = generator;
        this.sourceSelector = sourceSelector;
        this.targetSetter = targetSetter;
        this.targetGetter = targetGetter;
    }

    public async Task BeforeWrite(DocumentWriteContext ctx, CancellationToken ct)
    {
        // Auto-embed applies to inserts and upserts (both write a fresh body), matching the original scope.
        // Updates are left alone — re-embedding on partial patches is opt-in territory.
        if (ctx.Operation is not (DocumentOperation.Insert or DocumentOperation.Upsert))
            return;
        if (ctx.Document is not T doc)
            return;

        // Skip if a vector is already attached — the caller may want to override by setting the field explicitly.
        if (this.targetGetter != null && this.targetGetter(doc).Length > 0)
            return;

        var text = this.sourceSelector(doc);
        if (string.IsNullOrEmpty(text))
            return;

        var gen = this.generator
            ?? ctx.Services.GetService<IEmbeddingGenerator<string, Embedding<float>>>()
            ?? throw new InvalidOperationException(
                $"AutoEmbedOnInsert<{typeof(T).Name}> could not resolve an IEmbeddingGenerator<string, Embedding<float>>. " +
                "Register one in DI, or use the AutoEmbedOnInsert overload that takes an explicit generator " +
                "(required for the container-free 'new DocumentStore(options)' path).");

        var embedding = await gen.GenerateAsync(text, cancellationToken: ct).ConfigureAwait(false);
        this.targetSetter(doc, embedding.Vector);
    }

    public Task AfterWrite(DocumentWriteContext ctx, CancellationToken ct) => Task.CompletedTask;
}
