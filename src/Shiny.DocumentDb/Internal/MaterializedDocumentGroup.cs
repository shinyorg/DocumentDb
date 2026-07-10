namespace Shiny.DocumentDb.Internal;

/// <summary>
/// Marker for a client-side (in-memory) group whose members are materialized in memory. The
/// <see cref="Sql"/> group aggregates compute real values against <see cref="Members"/> instead of
/// throwing, so the in-memory providers (LiteDB / IndexedDB) can evaluate a compiled grouped projection
/// directly.
/// </summary>
internal interface IMaterializedGroup<out T>
{
    IReadOnlyList<T> Members { get; }
}

internal sealed class MaterializedDocumentGroup<TKey, T> : IDocumentGroup<TKey, T>, IMaterializedGroup<T>
{
    public MaterializedDocumentGroup(TKey key, IReadOnlyList<T> members)
    {
        this.Key = key;
        this.Members = members;
    }

    public TKey Key { get; }
    public IReadOnlyList<T> Members { get; }
}

/// <summary>Numeric folds used by the <see cref="Sql"/> group aggregates on the in-memory path.</summary>
internal static class GroupAggregateMath
{
    public static TValue Sum<TValue>(IEnumerable<TValue> values)
    {
        if (typeof(TValue) == typeof(double) || typeof(TValue) == typeof(float))
        {
            var sum = values.Sum(v => Convert.ToDouble(v));
            return (TValue)Convert.ChangeType(sum, typeof(TValue));
        }
        decimal acc = 0m;
        foreach (var v in values)
            acc += Convert.ToDecimal(v);
        return (TValue)Convert.ChangeType(acc, Nullable.GetUnderlyingType(typeof(TValue)) ?? typeof(TValue));
    }

    public static TValue Min<TValue>(IEnumerable<TValue> values)
    {
        var list = values.ToList();
        return list.Count == 0 ? default! : list.Min()!;
    }

    public static TValue Max<TValue>(IEnumerable<TValue> values)
    {
        var list = values.ToList();
        return list.Count == 0 ? default! : list.Max()!;
    }

    public static double Avg<T>(IReadOnlyList<T> members, Func<T, object?> selector)
        => members.Count == 0 ? 0d : members.Average(m => Convert.ToDouble(selector(m)));
}
