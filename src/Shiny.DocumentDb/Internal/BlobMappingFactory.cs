using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// Builds <see cref="BlobMapping"/> instances from the user-facing <c>MapBlob</c> / <c>MapBlobCollection</c>
/// overloads, mirroring <see cref="ComputedMappingFactory"/>: an expression overload for convenience and an
/// AOT-clean overload taking explicit accessor delegates.
/// </summary>
public static class BlobMappingFactory
{
    [UnconditionalSuppressMessage("Trimming", "IL2075", Justification = "Property is resolved by name from a user-provided expression over a user-constructed type; not subject to trimming.")]
    public static BlobMapping FromExpression<T>(Expression<Func<T, DocumentBlob?>> property, BlobOptions options)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(property);
        ArgumentNullException.ThrowIfNull(options);

        var name = ExtractMemberName(property);
        var propInfo = typeof(T).GetProperty(name)
            ?? throw new ArgumentException($"Property '{name}' not found on type '{typeof(T).Name}'.", nameof(property));
        if (!propInfo.CanWrite)
            throw new ArgumentException(
                $"Blob property '{name}' on '{typeof(T).Name}' must have a setter so the payload metadata can be populated on read.",
                nameof(property));

        return new BlobMapping
        {
            DocumentType = typeof(T),
            PropertyName = name,
            IsCollection = false,
            Key = string.IsNullOrWhiteSpace(options.Key) ? name : options.Key,
            ComputeHash = options.ComputeHash,
            MaxSize = options.MaxSize,
            GetValue = obj => propInfo.GetValue(obj),
            SetValue = (obj, val) => propInfo.SetValue(obj, val)
        };
    }

    [UnconditionalSuppressMessage("Trimming", "IL2075", Justification = "Property is resolved by name from a user-provided expression over a user-constructed type; not subject to trimming.")]
    public static BlobMapping FromCollectionExpression<T>(
        Expression<Func<T, DocumentBlobCollection?>> property, BlobOptions options) where T : class
    {
        ArgumentNullException.ThrowIfNull(property);
        ArgumentNullException.ThrowIfNull(options);

        if (!string.IsNullOrWhiteSpace(options.Key))
            throw new ArgumentException(
                "A blob collection cannot declare a fixed Key — each item carries its own key so reordering the list does not re-point sidecar rows.",
                nameof(options));

        var name = ExtractMemberName(property);
        var propInfo = typeof(T).GetProperty(name)
            ?? throw new ArgumentException($"Property '{name}' not found on type '{typeof(T).Name}'.", nameof(property));

        return new BlobMapping
        {
            DocumentType = typeof(T),
            PropertyName = name,
            IsCollection = true,
            Key = null,
            ComputeHash = options.ComputeHash,
            MaxSize = options.MaxSize,
            GetValue = obj => propInfo.GetValue(obj),
            SetValue = (obj, val) => propInfo.SetValue(obj, val)
        };
    }

    /// <summary>AOT-clean overload: no reflection on the read/write path.</summary>
    public static BlobMapping FromAccessors<T>(
        string propertyName,
        Func<T, DocumentBlob?> getter,
        Action<T, DocumentBlob?> setter,
        BlobOptions options) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(propertyName);
        ArgumentNullException.ThrowIfNull(getter);
        ArgumentNullException.ThrowIfNull(setter);
        ArgumentNullException.ThrowIfNull(options);

        return new BlobMapping
        {
            DocumentType = typeof(T),
            PropertyName = propertyName,
            IsCollection = false,
            Key = string.IsNullOrWhiteSpace(options.Key) ? propertyName : options.Key,
            ComputeHash = options.ComputeHash,
            MaxSize = options.MaxSize,
            GetValue = obj => getter((T)obj),
            SetValue = (obj, val) => setter((T)obj, (DocumentBlob?)val)
        };
    }

    /// <summary>AOT-clean collection overload: no reflection on the read/write path.</summary>
    public static BlobMapping FromCollectionAccessors<T>(
        string propertyName,
        Func<T, DocumentBlobCollection?> getter,
        Action<T, DocumentBlobCollection?> setter,
        BlobOptions options) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(propertyName);
        ArgumentNullException.ThrowIfNull(getter);
        ArgumentNullException.ThrowIfNull(setter);
        ArgumentNullException.ThrowIfNull(options);

        return new BlobMapping
        {
            DocumentType = typeof(T),
            PropertyName = propertyName,
            IsCollection = true,
            Key = null,
            ComputeHash = options.ComputeHash,
            MaxSize = options.MaxSize,
            GetValue = obj => getter((T)obj),
            SetValue = (obj, val) => setter((T)obj, (DocumentBlobCollection?)val)
        };
    }

    static string ExtractMemberName(LambdaExpression expression)
    {
        var body = expression.Body;
        if (body is UnaryExpression { NodeType: ExpressionType.Convert } unary)
            body = unary.Operand;

        if (body is MemberExpression member)
            return member.Member.Name;

        throw new ArgumentException("Expression must be a simple property accessor (e.g. x => x.Pdf).", nameof(expression));
    }
}
