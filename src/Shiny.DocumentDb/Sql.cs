namespace Shiny.DocumentDb;

/// <summary>
/// Marker class for SQL aggregate functions used in expression trees.
/// These methods are never executed — they exist only for expression-tree translation
/// in aggregate projections.
/// </summary>
public static class Sql
{
    /// <summary>Translates to SQL COUNT(*).</summary>
    public static int Count() => throw new InvalidOperationException("Sql.Count() is only supported inside aggregate expression trees.");

    /// <summary>Translates to SQL MAX(expression).</summary>
    public static TValue Max<TValue>(TValue value) => throw new InvalidOperationException("Sql.Max() is only supported inside aggregate expression trees.");

    /// <summary>Translates to SQL MIN(expression).</summary>
    public static TValue Min<TValue>(TValue value) => throw new InvalidOperationException("Sql.Min() is only supported inside aggregate expression trees.");

    /// <summary>Translates to SQL SUM(expression).</summary>
    public static TValue Sum<TValue>(TValue value) => throw new InvalidOperationException("Sql.Sum() is only supported inside aggregate expression trees.");

    /// <summary>Translates to SQL AVG(expression).</summary>
    public static double Avg(object value) => throw new InvalidOperationException("Sql.Avg() is only supported inside aggregate expression trees.");
}
