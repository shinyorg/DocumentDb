namespace Shiny.DocumentDb;

/// <summary>
/// Controls how <c>null</c> values are treated when building an <c>IN</c> / <c>NOT IN</c> predicate
/// with <see cref="DocumentQueryExtensions.WhereIn{T, TValue}"/> /
/// <see cref="DocumentQueryExtensions.WhereNotIn{T, TValue}"/>.
/// </summary>
public enum NullHandling
{
    /// <summary>
    /// Pass the value set through unchanged. You inherit the underlying store's native three-valued
    /// logic — including the classic <c>NOT IN (…, NULL)</c> trap that matches no rows. Faithful to
    /// raw SQL, but a <c>null</c> in the set means different things across providers, so prefer
    /// <see cref="Ignore"/> or <see cref="Match"/> unless you specifically want native semantics.
    /// </summary>
    Raw,

    /// <summary>
    /// Strip <c>null</c> elements from the value set before comparing. The safe default: it removes
    /// the <c>NOT IN</c> footgun, and a <c>null</c> field simply never matches an <c>IN</c> (and is
    /// excluded by <c>NOT IN</c>, following the store default).
    /// </summary>
    Ignore,

    /// <summary>
    /// Strip <c>null</c> elements like <see cref="Ignore"/>, but treat the presence of a <c>null</c>
    /// element as explicit intent about <c>null</c> <em>fields</em>:
    /// <list type="bullet">
    /// <item><c>WhereIn</c> → a <c>null</c> field is a match (<c>… OR field IS NULL</c>).</item>
    /// <item><c>WhereNotIn</c> → a <c>null</c> field is excluded (<c>… AND field IS NOT NULL</c>).</item>
    /// </list>
    /// </summary>
    Match
}
