namespace Shiny.DocumentDb;

/// <summary>
/// Logical scalar functions the query layer can translate. Each backend renders these via its dialect
/// (relational/Cosmos through <c>IDatabaseProvider.TranslateScalar</c>, Mongo through <c>$expr</c>), so the
/// recognition layer stays backend-agnostic. Public because it is a parameter of the
/// <see cref="IDatabaseProvider.TranslateScalar"/> dialect seam.
/// </summary>
public enum ScalarFn
{
    // ── String ──
    Lower,
    Upper,
    Length,
    Trim,
    TrimStart,
    TrimEnd,
    Substring,
    Replace,
    IndexOf,
    Concat,

    // ── Math ──
    Abs,
    Ceiling,
    Floor,
    Round,
    Sqrt,
    Pow,
    Sign,

    // ── Date / time ──
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,

    // ── Phonetic / fuzzy ──
    Soundex,
}
