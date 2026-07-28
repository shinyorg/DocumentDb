using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;

namespace ShinyDocDbMyAdmin.Services;

/// <summary>
/// Builds new documents that look like the ones already there.
/// </summary>
/// <remarks>
/// <para>
/// Every value comes from what the real documents contain: a number lands inside the observed range,
/// a date inside the observed span, a categorical field is drawn from the set actually used. That is
/// the point — data generated from a schema alone produces plausible <i>types</i> and implausible
/// <i>data</i>, which is no use for judging a query plan or eyeballing a grid.
/// </para>
/// <para>
/// Seeded, so a preview and the commit that follows it produce byte-identical documents. Showing
/// someone one set of records and then writing a different set would make the preview a decoration.
/// </para>
/// </remarks>
public sealed partial class DocumentGenerator
{
    readonly DocumentShape shape;
    readonly Random random;
    readonly IdPattern idPattern;

    public DocumentGenerator(DocumentShape shape, int seed)
    {
        this.shape = shape;
        this.random = new Random(seed);
        this.idPattern = IdPattern.Detect(shape);
    }

    /// <summary>The seed this instance was built with, so a preview can be reproduced exactly.</summary>
    public static int NewSeed() => Random.Shared.Next();

    /// <summary>
    /// How ids will actually be minted. The id field never follows the strategy its <i>values</i>
    /// suggest — it is always replaced — so the shape table has to describe this rather than report
    /// the field as the free text it looks like.
    /// </summary>
    public static string DescribeIdStrategy(DocumentShape shape)
    {
        var pattern = IdPattern.Detect(shape);
        if (pattern.Guid || pattern.Prefix is null)
            return "new guid";

        var first = pattern.Next(0, Random.Shared)!.GetValue<string>();
        return $"sequence from {first}";
    }

    public IReadOnlyList<JsonObject> Generate(int count)
    {
        var results = new List<JsonObject>(count);
        for (var i = 0; i < count; i++)
        {
            var document = (JsonObject)this.Build(this.shape.Root, required: true)!;

            // The id is never sampled: reusing an observed one would collide with the document it
            // came from, and every other field is free to repeat.
            document[this.shape.IdProperty] = this.idPattern.Next(i, this.random);
            results.Add(document);
        }

        return results;
    }

    JsonNode? Build(NodeShape node, bool required)
    {
        if (!required && this.random.NextDouble() > node.PresenceIn(this.shape.Root.Observations))
            return null;

        if (node.IsObject)
        {
            var obj = new JsonObject();
            foreach (var name in node.PropertyOrder)
            {
                var child = node.Properties[name];
                var value = this.Build(child, required: child.PresenceIn(node.Observations) >= 1);

                // A missing optional field is left out rather than written as null - that is what
                // "absent from some documents" looks like in the documents themselves.
                if (value is not null || child.Kinds.ContainsKey(JsonValueKind.Null))
                    obj[name] = value;
            }

            return obj;
        }

        if (node.IsArray)
        {
            var length = node.MinLength == int.MaxValue
                ? 0
                : this.random.Next(node.MinLength, node.MaxLength + 1);

            var array = new JsonArray();
            for (var i = 0; i < length && node.Element is not null; i++)
                array.Add(this.Build(node.Element, required: true));

            return array;
        }

        return this.Scalar(node);
    }

    JsonNode? Scalar(NodeShape node)
    {
        // A GUID field gets a fresh one: they are identifiers, and repeating them is exactly the
        // failure that makes generated data useless for testing joins.
        if (node.IsGuid)
            return JsonValue.Create(Guid.NewGuid().ToString());

        if (node.IsDate && node.MinDate is { } from && node.MaxDate is { } to)
            return JsonValue.Create(this.Date(from, to, node.DateFormat));

        if (node.DominantKind == JsonValueKind.Number && node is { Min: { } min, Max: { } max })
            return this.Number(min, max, node.AllIntegral);

        if (node.DominantKind is JsonValueKind.True or JsonValueKind.False)
        {
            // Weighted by how often each was actually true, so a mostly-false flag stays mostly false.
            var trues = node.Kinds.GetValueOrDefault(JsonValueKind.True);
            var total = trues + node.Kinds.GetValueOrDefault(JsonValueKind.False);
            return JsonValue.Create(total > 0 && this.random.Next(total) < trues);
        }

        // Everything else - categorical or free text - is drawn from the values actually seen. For a
        // saturated field that is a sample of the real corpus rather than the whole of it, which is
        // the honest limit of generating without a model: the shapes repeat.
        return node.Values.Count > 0
            ? node.Values[this.random.Next(node.Values.Count)].DeepClone()
            : null;
    }

    JsonNode Number(double min, double max, bool integral)
    {
        if (integral)
        {
            var lo = (long)Math.Floor(min);
            var hi = (long)Math.Ceiling(max);
            return JsonValue.Create(hi <= lo ? lo : this.random.NextInt64(lo, hi + 1));
        }

        var value = min + this.random.NextDouble() * (max - min);
        return JsonValue.Create(Math.Round(value, 2));
    }

    string Date(DateTimeOffset from, DateTimeOffset to, string? format)
    {
        var span = to - from;
        var at = span <= TimeSpan.Zero ? from : from + TimeSpan.FromTicks((long)(this.random.NextDouble() * span.Ticks));

        return format switch
        {
            null or "O" => at.ToString("O", CultureInfo.InvariantCulture),
            _ => at.ToString(format, CultureInfo.InvariantCulture)
        };
    }

    /// <summary>
    /// How to mint ids that continue what is already there.
    /// </summary>
    /// <remarks>
    /// A store whose ids read <c>art-0001 … art-0400</c> should carry on at <c>art-0401</c>, not
    /// start emitting GUIDs beside them — the ids are the first thing anyone looks at, and a grid
    /// where half of them follow a scheme and half do not reads as corruption.
    /// </remarks>
    sealed record IdPattern(string? Prefix, int? Width, long Next0, bool Guid)
    {
        public static IdPattern Detect(DocumentShape shape)
        {
            if (!shape.Root.Properties.TryGetValue(shape.IdProperty, out var id) || id.Values.Count == 0)
                return new IdPattern(null, null, 0, Guid: true);

            var texts = id.Values
                .Where(v => v.GetValueKind() == JsonValueKind.String)
                .Select(v => v.GetValue<string>())
                .ToList();

            if (texts.Count == 0 || texts.Count != id.Values.Count)
                return new IdPattern(null, null, 0, Guid: true);

            if (texts.All(t => System.Guid.TryParse(t, out _)))
                return new IdPattern(null, null, 0, Guid: true);

            var matches = texts.Select(t => SequencePattern().Match(t)).ToList();
            if (matches.Any(m => !m.Success))
                return new IdPattern(null, null, 0, Guid: true);

            var prefixes = matches.Select(m => m.Groups["prefix"].Value).Distinct().ToList();
            if (prefixes.Count != 1)
                return new IdPattern(null, null, 0, Guid: true);

            var digits = matches.Select(m => m.Groups["num"].Value).ToList();
            var highest = digits.Select(d => long.Parse(d, CultureInfo.InvariantCulture)).Max();

            // Zero-padded only when the sample is consistently padded; otherwise plain numbers.
            var width = digits.Select(d => d.Length).Distinct().Count() == 1 && digits[0].StartsWith('0')
                ? digits[0].Length
                : (int?)null;

            return new IdPattern(prefixes[0], width, highest + 1, Guid: false);
        }

        public JsonNode Next(int index, Random random)
        {
            if (this.Guid || this.Prefix is null)
                return JsonValue.Create(System.Guid.NewGuid().ToString());

            var value = this.Next0 + index;
            var text = this.Width is { } width
                ? value.ToString(CultureInfo.InvariantCulture).PadLeft(width, '0')
                : value.ToString(CultureInfo.InvariantCulture);

            return JsonValue.Create(this.Prefix + text);
        }
    }

    /// <summary>An id ending in digits, e.g. <c>art-0042</c> or <c>order_7</c>.</summary>
    [GeneratedRegex(@"^(?<prefix>.*?)(?<num>\d+)$", RegexOptions.Compiled)]
    private static partial Regex SequencePattern();
}
