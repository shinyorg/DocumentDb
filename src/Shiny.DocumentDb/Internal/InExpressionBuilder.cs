using System.Collections;
using System.Linq.Expressions;
using System.Reflection;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// Builds the single canonical <c>IN</c> lowering shared by the typed
/// <see cref="DocumentQueryExtensions.WhereIn{T, TValue}"/> front door and the string filter
/// parser (<see cref="FilterExpressionParser"/>): a boxed
/// <see cref="Enumerable.Contains{TSource}(IEnumerable{TSource}, TSource)"/> call that every
/// provider already recognizes (relational → <c>IN (…)</c>, Cosmos → <c>IN</c>, Mongo → <c>$in</c>,
/// LiteDB/IndexedDB → in-memory <c>Contains</c>).
/// </summary>
/// <remarks>
/// The element type is erased to <see cref="object"/> on purpose. The closed
/// <c>Enumerable.Contains&lt;object&gt;</c> handle is captured from a method group (no reflection
/// scan, no <c>MakeGenericMethod</c> over value types), so this path stays AOT/trim-safe — the same
/// guarantee <see cref="FilterExpressionParser"/> documents. Boxed value-equality is correct for the
/// primitive leaf types the store supports (int, long, Guid, enum, DateTime, string, …).
/// </remarks>
static class InExpressionBuilder
{
    static readonly MethodInfo ContainsObject =
        new Func<IEnumerable<object>, object, bool>(Enumerable.Contains).Method;

    /// <summary>
    /// Builds <c>values.Contains((object)member)</c> with <paramref name="nulls"/> applied. Under
    /// <see cref="NullHandling.Match"/>, a <c>null</c> in <paramref name="rawValues"/> is OR-ed in as
    /// an explicit <c>member IS NULL</c> check (the caller negates the whole expression for
    /// <c>WhereNotIn</c>, which De&#160;Morgans into <c>… AND member IS NOT NULL</c>).
    /// </summary>
    public static Expression Build(Expression member, IEnumerable rawValues, NullHandling nulls)
    {
        var (values, hadNull) = Normalize(rawValues, nulls);

        var boxedMember = member.Type == typeof(object)
            ? member
            : Expression.Convert(member, typeof(object));

        Expression contains = Expression.Call(
            ContainsObject,
            Expression.Constant(values, typeof(IEnumerable<object>)),
            boxedMember);

        if (nulls == NullHandling.Match && hadNull)
            return Expression.OrElse(contains, BuildNullCheck(member, isNull: true));

        return contains;
    }

    /// <summary>
    /// Builds an <c>x == null</c> / <c>x != null</c> check shaped so the provider visitors recognize
    /// it as a null comparison (the member is converted to <see cref="object"/> and compared against a
    /// typed <c>null</c> constant).
    /// </summary>
    public static Expression BuildNullCheck(Expression member, bool isNull)
    {
        var asObject = Expression.Convert(member, typeof(object));
        var nullConstant = Expression.Constant(null, typeof(object));
        return isNull
            ? Expression.Equal(asObject, nullConstant)
            : Expression.NotEqual(asObject, nullConstant);
    }

    static (List<object?> Values, bool HadNull) Normalize(IEnumerable rawValues, NullHandling nulls)
    {
        var values = new List<object?>();
        var hadNull = false;

        foreach (var v in rawValues)
        {
            if (v is null)
            {
                hadNull = true;
                if (nulls == NullHandling.Raw)
                    values.Add(null);   // Raw keeps nulls — you inherit native three-valued logic
            }
            else
            {
                values.Add(v);
            }
        }

        return (values, hadNull);
    }
}
