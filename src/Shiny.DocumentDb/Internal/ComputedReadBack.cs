namespace Shiny.DocumentDb.Internal;

/// <summary>
/// Populates computed (<c>[JsonIgnore]</c>'d) properties on documents after they are read, so the
/// derived value is present on the returned object. Used by the document providers, whose query results
/// are materialized objects rather than a SQL projection.
/// </summary>
public static class ComputedReadBack
{
    public static void Apply<T>(IEnumerable<T> documents, IReadOnlyList<ComputedMapping> mappings) where T : class
    {
        if (mappings.Count == 0)
            return;

        foreach (var document in documents)
        {
            if (document is null)
                continue;
            for (var i = 0; i < mappings.Count; i++)
                mappings[i].SetValue(document, mappings[i].Compute(document));
        }
    }
}
