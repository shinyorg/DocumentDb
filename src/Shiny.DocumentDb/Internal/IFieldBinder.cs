using System.Linq.Expressions;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// The seam that lets one filter grammar serve two field-resolution strategies: a typed collection resolves
/// a path against the document type's <c>JsonTypeInfo</c> and knows every leaf's CLR type up front, while a
/// schema-free collection resolves it to a bare JSON path whose type must be <em>inferred</em> as the
/// expression is built.
/// </summary>
/// <remarks>
/// <para>
/// Inference cannot be deferred to lowering, because both failures fire at expression-<em>construction</em>
/// time: <c>Expression.GreaterThan(objectTyped, objectTyped)</c> throws (there is no <c>&gt;</c> on
/// <see cref="object"/>), and coercing a literal against <see cref="object"/> matches no branch and silently
/// binds the raw string. So every site in the parser that asserts, converts, or compares against a leaf type
/// routes through this interface instead of doing it inline.
/// </para>
/// <para>
/// Every method on the typed implementation is the pre-existing behaviour verbatim — usually the identity
/// function — so the typed grammar is unchanged byte-for-byte.
/// </para>
/// </remarks>
interface IFieldBinder
{
    /// <summary>Resolves a field path to an operand expression and its leaf CLR type.</summary>
    (Expression Body, Type LeafType) Resolve(string path);

    /// <summary>True when <paramref name="leafType"/> is the schema-free "not yet inferred" sentinel.</summary>
    bool IsUnresolved(Type leafType);

    /// <summary>
    /// A function that <em>requires</em> its argument to be <paramref name="required"/> (e.g. <c>lower</c>
    /// needs a string). Typed asserts; schema-free retypes the field.
    /// </summary>
    Expression Require(Expression operand, Type leafType, Type required, string func, int position);

    /// <summary>A function that numerically converts its argument (<c>abs</c>, <c>round</c>, <c>pow</c>, …).</summary>
    Expression NumericArg(Expression operand, Type leafType);

    /// <summary>
    /// A function that reads a member off its argument (the date parts, which compile to
    /// <c>Expression.Property(arg, "Year")</c> and so need the argument to already be that type).
    /// </summary>
    Expression MemberArg(Expression operand, Type leafType, Type required);

    /// <summary>
    /// The CLR type a literal should be coerced to. Typed always answers the leaf type; schema-free answers
    /// the literal's own natural type when the leaf is still unresolved — this is what makes
    /// <c>Where("total &gt; 100")</c> bind an <see cref="int"/> and extract numerically.
    /// </summary>
    Type LiteralType(Type leafType, Type naturalType);

    /// <summary>
    /// Adapts a field operand once the other side's type is known (a comparison's literal, an
    /// <c>in (…)</c> list, or a function-valued right-hand side).
    /// </summary>
    Expression AdaptTo(Expression operand, Type type);

    /// <summary>The stored-geometry operand of a geo function. Schema-free throws — geometry needs a mapping.</summary>
    Expression GeometryArg(Expression operand, Type leafType, int position);

    /// <summary>The enum type behind a <c>hasflag</c> field. Schema-free throws — there is no CLR enum.</summary>
    Type EnumArg(Type leafType, string func, int position);

    /// <summary>
    /// Guards the functions that need a <c>Map*Property&lt;T&gt;</c> registration keyed by CLR type
    /// (full-text). Typed is a no-op — an unmapped type still throws further down, exactly as before.
    /// </summary>
    void RequireMapping(string func, int position);
}
