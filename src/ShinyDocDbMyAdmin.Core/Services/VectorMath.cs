using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using Shiny.DocumentDb;

namespace ShinyDocDbMyAdmin.Services;

/// <summary>
/// Reading embeddings out of JSON and comparing them, with no database involved.
/// </summary>
/// <remarks>
/// The tool computes distances itself rather than pushing an ANN query down to the engine. That is a
/// deliberate trade: <c>BuildVectorSearchSql</c> needs a registered <c>VectorMapping</c> (dimensions
/// and metric), which an admin tool pointed at someone else's database does not have, and on SQLite it
/// needs the sqlite-vec extension loaded. A brute-force scan over a bounded number of documents needs
/// neither, gives an exact answer rather than an approximate one, and behaves identically on every
/// backend - including a table whose sidecar is missing or stale, which is exactly when you are
/// looking. The cost is the scan limit, which the UI states rather than hides.
/// </remarks>
public static class VectorMath
{
    /// <summary>
    /// The shortest numeric array treated as an embedding. Low enough to catch toy vectors in a test
    /// database, high enough that an RGB triplet or a lat/lng pair is not mistaken for one.
    /// </summary>
    public const int MinDimensions = 8;

    /// <summary>Reads a JSON array of numbers as an embedding. Null when the node is not one.</summary>
    public static float[]? Read(JsonNode? node, int minDimensions = MinDimensions)
    {
        if (node is not JsonArray array || array.Count < minDimensions)
            return null;

        var values = new float[array.Count];
        for (var i = 0; i < array.Count; i++)
        {
            if (array[i] is not JsonValue value || value.GetValueKind() != JsonValueKind.Number)
                return null;

            // A number too large for float is still a number; it saturates to infinity and is
            // reported as non-finite rather than silently dropping the whole vector.
            values[i] = value.TryGetValue<float>(out var f) ? f : float.NaN;
        }

        return values;
    }

    /// <summary>True when a node has the shape of an embedding, without materialising it.</summary>
    public static bool LooksLikeVector(JsonNode? node, int minDimensions = MinDimensions)
        => node is JsonArray array
           && array.Count >= minDimensions
           && array.All(x => x is JsonValue v && v.GetValueKind() == JsonValueKind.Number);

    /// <summary>
    /// Parses a vector typed or pasted by a user: a JSON array, or bare numbers separated by commas
    /// or whitespace. Both forms turn up - one is copied out of a document, the other out of a log.
    /// </summary>
    public static float[] Parse(string text)
    {
        if (string.IsNullOrWhiteSpace(text))
            throw new FormatException("Enter a vector.");

        var trimmed = text.Trim().Trim('[', ']');
        var parts = trimmed.Split([',', ' ', '\t', '\r', '\n'], StringSplitOptions.RemoveEmptyEntries);
        var values = new float[parts.Length];

        for (var i = 0; i < parts.Length; i++)
        {
            if (!float.TryParse(parts[i], NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed))
                throw new FormatException($"'{parts[i]}' is not a number.");

            values[i] = parsed;
        }

        if (values.Length == 0)
            throw new FormatException("Enter a vector.");

        return values;
    }

    /// <summary>Renders a vector as the JSON array form every provider's literal syntax accepts.</summary>
    public static string Format(IReadOnlyList<float> values)
        => "[" + string.Join(",", values.Select(v => v.ToString("R", CultureInfo.InvariantCulture))) + "]";

    /// <summary>
    /// Distance between two vectors under one metric, oriented so that <b>smaller is nearer</b> -
    /// matching how every provider's search SQL orders its results.
    /// </summary>
    /// <exception cref="ArgumentException">The vectors have different dimensions.</exception>
    public static double Distance(ReadOnlySpan<float> a, ReadOnlySpan<float> b, VectorDistance metric)
    {
        if (a.Length != b.Length)
            throw new ArgumentException($"Dimension mismatch: {a.Length} vs {b.Length}.", nameof(b));

        return metric switch
        {
            VectorDistance.Cosine => 1.0 - Cosine(a, b),
            VectorDistance.Euclidean => Euclidean(a, b),
            // Negated so that ORDER BY ascending still means "nearest", as the DuckDB provider does.
            VectorDistance.DotProduct => -Dot(a, b),
            VectorDistance.Hamming => Hamming(a, b),
            _ => throw new ArgumentOutOfRangeException(nameof(metric), metric, null)
        };
    }

    /// <summary>Cosine similarity in [-1, 1]. Zero when either vector has no magnitude.</summary>
    public static double Cosine(ReadOnlySpan<float> a, ReadOnlySpan<float> b)
    {
        double dot = 0, aa = 0, bb = 0;
        for (var i = 0; i < a.Length; i++)
        {
            dot += (double)a[i] * b[i];
            aa += (double)a[i] * a[i];
            bb += (double)b[i] * b[i];
        }

        var magnitude = Math.Sqrt(aa) * Math.Sqrt(bb);
        return magnitude == 0 ? 0 : dot / magnitude;
    }

    public static double Euclidean(ReadOnlySpan<float> a, ReadOnlySpan<float> b)
    {
        double sum = 0;
        for (var i = 0; i < a.Length; i++)
        {
            var d = (double)a[i] - b[i];
            sum += d * d;
        }

        return Math.Sqrt(sum);
    }

    public static double Dot(ReadOnlySpan<float> a, ReadOnlySpan<float> b)
    {
        double sum = 0;
        for (var i = 0; i < a.Length; i++)
            sum += (double)a[i] * b[i];

        return sum;
    }

    /// <summary>Count of positions where the two differ, the float analogue of a bitwise Hamming distance.</summary>
    static double Hamming(ReadOnlySpan<float> a, ReadOnlySpan<float> b)
    {
        var different = 0;
        for (var i = 0; i < a.Length; i++)
        {
            if (a[i] != b[i])
                different++;
        }

        return different;
    }

    /// <summary>Euclidean length. 1.0 (near enough) means the embedding was normalised before storage.</summary>
    public static double Norm(ReadOnlySpan<float> values)
    {
        double sum = 0;
        foreach (var v in values)
            sum += (double)v * v;

        return Math.Sqrt(sum);
    }

    /// <summary>
    /// Whether a norm is close enough to 1 to call the vector normalised. The tolerance is loose on
    /// purpose: the question being answered is "did whoever wrote this normalise it", and float32
    /// round-off over 1536 components moves the norm well past an exact comparison.
    /// </summary>
    public static bool IsNormalised(double norm) => Math.Abs(norm - 1.0) < 0.01;
}
