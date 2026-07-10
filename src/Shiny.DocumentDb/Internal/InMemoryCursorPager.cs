namespace Shiny.DocumentDb.Internal;

/// <summary>One sort key for in-memory cursor pagination: a value getter and its direction.</summary>
/// <typeparam name="T">The document type.</typeparam>
public readonly record struct CursorSortKey<T>(Func<T, object?> Get, bool Descending);

/// <summary>
/// Shared keyset (cursor) pager for the providers that evaluate queries in-memory (LiteDB, IndexedDB). It
/// sorts the matching documents by the query's <c>OrderBy</c> keys plus a mandatory <c>Id</c> tiebreaker,
/// seeks past the row named by the incoming cursor, and returns one forward page — the in-memory analogue of
/// the relational OR-chain keyset. Uses the same <see cref="CursorCodec"/> token format as the relational
/// engine so the public contract is identical.
/// </summary>
static class InMemoryCursorPager
{
    const int MaxCursorTake = 10_000;

    public static CursorPage<T> Page<T>(
        IEnumerable<T> matching,
        IReadOnlyList<CursorSortKey<T>> keys,
        Func<T, string> getId,
        string shapeSpec,
        string? cursor,
        int take) where T : class
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(take);
        if (take > MaxCursorTake)
            throw new ArgumentOutOfRangeException(nameof(take), take,
                $"Cursor page size must not exceed {MaxCursorTake}.");

        var shapeHash = CursorCodec.ComputeShapeHash(shapeSpec);

        var sorted = matching.ToList();
        sorted.Sort((a, b) => CompareRows(a, b, keys, getId));

        var startIndex = 0;
        if (cursor != null)
        {
            var values = CursorCodec.DecodeKeyset(cursor, shapeHash);
            if (values.Count != keys.Count + 1)
                throw new InvalidOperationException("The cursor is malformed or corrupt.");

            // Skip every row up to and including the one the cursor names (the first row that sorts strictly
            // after the cursor tuple is where this page begins).
            startIndex = sorted.FindIndex(row => CompareRowToCursor(row, values, keys, getId) > 0);
            if (startIndex < 0)
                startIndex = sorted.Count;
        }

        var slice = new List<T>(take + 1);
        for (var i = startIndex; i < sorted.Count && slice.Count < take + 1; i++)
            slice.Add(sorted[i]);

        if (slice.Count <= take)
            return new CursorPage<T>(slice, null);

        slice.RemoveAt(take);
        var last = slice[take - 1];
        var cursorValues = new object?[keys.Count + 1];
        for (var i = 0; i < keys.Count; i++)
            cursorValues[i] = keys[i].Get(last);
        cursorValues[keys.Count] = getId(last);

        return new CursorPage<T>(slice, CursorCodec.EncodeKeyset(shapeHash, cursorValues));
    }

    static int CompareRows<T>(T a, T b, IReadOnlyList<CursorSortKey<T>> keys, Func<T, string> getId)
    {
        foreach (var key in keys)
        {
            var cmp = CompareValues(key.Get(a), key.Get(b));
            if (cmp != 0)
                return key.Descending ? -cmp : cmp;
        }
        return string.CompareOrdinal(getId(a), getId(b));
    }

    static int CompareRowToCursor<T>(T row, IReadOnlyList<object?> cursorValues, IReadOnlyList<CursorSortKey<T>> keys, Func<T, string> getId)
    {
        for (var i = 0; i < keys.Count; i++)
        {
            var cmp = CompareValues(keys[i].Get(row), cursorValues[i]);
            if (cmp != 0)
                return keys[i].Descending ? -cmp : cmp;
        }
        return string.CompareOrdinal(getId(row), cursorValues[keys.Count] as string);
    }

    static int CompareValues(object? a, object? b)
    {
        var na = CursorCodec.NormalizeForCompare(a);
        var nb = CursorCodec.NormalizeForCompare(b);
        if (na == null && nb == null) return 0;
        if (na == null) return -1;   // nulls sort first (ascending), matching Comparer<object>.Default
        if (nb == null) return 1;
        return Comparer<object>.Default.Compare(na, nb);
    }
}
