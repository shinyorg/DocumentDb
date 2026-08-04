namespace Shiny.DocumentDb;

/// <summary>Typed sugar over the JSON-collection lane.</summary>
public static class DocumentStoreCollectionExtensions
{
    /// <summary>
    /// Opens the late-bound JSON view over <typeparamref name="T"/> — the generic spelling of
    /// <see cref="IDocumentStore.Collection(Type)"/>, with identical behaviour.
    /// </summary>
    public static IJsonDocumentCollection Collection<T>(this IDocumentStore store) where T : class
    {
        ArgumentNullException.ThrowIfNull(store);
        return store.Collection(typeof(T));
    }
}
