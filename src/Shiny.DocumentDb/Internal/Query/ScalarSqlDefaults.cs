namespace Shiny.DocumentDb.Internal.Query;

/// <summary>
/// ANSI-portable rendering of <see cref="ScalarFn"/> values (correct as-is for SQLite and DuckDB).
/// Lives in a static helper so a provider can override <see cref="IDatabaseProvider.TranslateScalar"/>
/// to handle only its dialect deltas and delegate everything else here, instead of restating the whole
/// switch. <paramref name="p"/> supplies <c>CastInteger</c>/<c>ConcatStrings</c>.
/// </summary>
public static class ScalarSqlDefaults
{
    public static string Translate(IDatabaseProvider p, ScalarFn fn, IReadOnlyList<string> args, Type resultType) => fn switch
    {
        ScalarFn.Lower => $"LOWER({args[0]})",
        ScalarFn.Upper => $"UPPER({args[0]})",
        ScalarFn.Length => $"LENGTH({args[0]})",
        ScalarFn.Trim => $"TRIM({args[0]})",
        ScalarFn.TrimStart => $"LTRIM({args[0]})",
        ScalarFn.TrimEnd => $"RTRIM({args[0]})",
        ScalarFn.Substring => args.Count == 3
            ? $"SUBSTR({args[0]}, {p.CastInteger(args[1])} + 1, {p.CastInteger(args[2])})"
            : $"SUBSTR({args[0]}, {p.CastInteger(args[1])} + 1)",
        ScalarFn.Replace => $"REPLACE({args[0]}, {args[1]}, {args[2]})",
        ScalarFn.IndexOf => $"(INSTR({args[0]}, {args[1]}) - 1)",
        ScalarFn.Concat => p.ConcatStrings([.. args]),
        ScalarFn.Abs => $"ABS({args[0]})",
        ScalarFn.Ceiling => $"CEIL({args[0]})",
        ScalarFn.Floor => $"FLOOR({args[0]})",
        ScalarFn.Round => args.Count == 2 ? $"ROUND({args[0]}, {args[1]})" : $"ROUND({args[0]})",
        ScalarFn.Sqrt => $"SQRT({args[0]})",
        ScalarFn.Pow => $"POWER({args[0]}, {args[1]})",
        ScalarFn.Sign => $"SIGN({args[0]})",
        ScalarFn.Year => $"CAST(EXTRACT(YEAR FROM CAST({args[0]} AS TIMESTAMP)) AS INTEGER)",
        ScalarFn.Month => $"CAST(EXTRACT(MONTH FROM CAST({args[0]} AS TIMESTAMP)) AS INTEGER)",
        ScalarFn.Day => $"CAST(EXTRACT(DAY FROM CAST({args[0]} AS TIMESTAMP)) AS INTEGER)",
        ScalarFn.Hour => $"CAST(EXTRACT(HOUR FROM CAST({args[0]} AS TIMESTAMP)) AS INTEGER)",
        ScalarFn.Minute => $"CAST(EXTRACT(MINUTE FROM CAST({args[0]} AS TIMESTAMP)) AS INTEGER)",
        ScalarFn.Second => $"CAST(EXTRACT(SECOND FROM CAST({args[0]} AS TIMESTAMP)) AS INTEGER)",
        ScalarFn.Soundex => p.SupportsSoundex
            ? $"SOUNDEX({args[0]})"
            : throw new NotSupportedException(
                "DocumentFunctions.Soundex() is not supported by this provider — it has no phonetic-matching " +
                "built-in (e.g. CockroachDB, or PostgreSQL without the fuzzystrmatch extension enabled via " +
                "EnableFuzzyStringMatch). Precompute the Soundex code in your app and store it as a normal, " +
                "indexed field instead."),
        _ => throw new NotSupportedException($"Scalar function '{fn}' is not supported by this provider.")
    };
}
