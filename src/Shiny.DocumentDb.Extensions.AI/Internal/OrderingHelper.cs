using System.Linq.Expressions;

namespace Shiny.DocumentDb.Extensions.AI.Internal;

static class OrderingHelper
{
    /// <summary>Builds a <c>x =&gt; (object)x.Property</c> expression for the given JSON field name.</summary>
    public static Expression<Func<T, object>>? BuildOrderBy<T>(string jsonFieldName, IReadOnlyList<DocumentField> fields)
    {
        foreach (var f in fields)
        {
            if (f.ClrProperty == null)
                continue;
            if (string.Equals(f.JsonName, jsonFieldName, StringComparison.Ordinal))
            {
                var parameter = Expression.Parameter(typeof(T), "x");
                Expression body = Expression.Property(parameter, f.ClrProperty);
                if (body.Type.IsValueType)
                    body = Expression.Convert(body, typeof(object));
                return Expression.Lambda<Func<T, object>>(body, parameter);
            }
        }
        return null;
    }
}
