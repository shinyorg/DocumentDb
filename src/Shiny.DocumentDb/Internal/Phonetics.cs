using System.Text;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// AOT-safe phonetic algorithms. One canonical implementation backs every mechanism: the in-memory
/// providers call it directly via <see cref="DocumentFunctions"/>, SQLite/DuckDB register it as a
/// connection UDF named <c>soundex</c>, and the compute-on-write stored-key pattern uses it too — so the
/// phonetic key is identical across providers (see the cross-provider determinism note in the design doc).
/// </summary>
static class Phonetics
{
    /// <summary>
    /// Classic American Soundex: the first letter followed by three encoded digits, right-padded with
    /// zeros. Returns <c>"0000"</c> when the input has no ASCII letters.
    /// </summary>
    public static string Soundex(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return "0000";

        var sb = new StringBuilder(4);
        var first = '\0';
        var prevCode = '0';

        foreach (var ch in value)
        {
            var c = char.ToUpperInvariant(ch);
            if (c is < 'A' or > 'Z')
                continue;

            var code = Encode(c);
            if (first == '\0')
            {
                first = c;
                prevCode = code;
                sb.Append(c);
                continue;
            }

            if (code != '0' && code != prevCode)
                sb.Append(code);

            // Vowels reset adjacency; H and W are transparent (don't reset the previous code).
            if (c is not ('H' or 'W'))
                prevCode = code;

            if (sb.Length >= 4)
                break;
        }

        if (first == '\0')
            return "0000";

        while (sb.Length < 4)
            sb.Append('0');

        return sb.ToString(0, 4);
    }

    static char Encode(char c) => c switch
    {
        'B' or 'F' or 'P' or 'V' => '1',
        'C' or 'G' or 'J' or 'K' or 'Q' or 'S' or 'X' or 'Z' => '2',
        'D' or 'T' => '3',
        'L' => '4',
        'M' or 'N' => '5',
        'R' => '6',
        _ => '0'
    };
}
