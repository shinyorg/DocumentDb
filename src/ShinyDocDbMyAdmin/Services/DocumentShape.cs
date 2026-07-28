using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace ShinyDocDbMyAdmin.Services;

/// <summary>
/// What a type's documents actually look like, in enough detail to make more of them.
/// </summary>
/// <remarks>
/// Deliberately a separate model from <c>InferredSchema</c>, which describes a type for a human
/// reading the Structure tab: flat, one level deep, and carrying an example rather than a
/// distribution. Generating needs the opposite — the whole nested tree, the range of every number,
/// the set every categorical field draws from — so the two would only be one type at the cost of
/// making both worse.
/// </remarks>
public sealed class NodeShape
{
    /// <summary>Distinct scalar values kept per node. Past this a field is treated as free-form rather than categorical.</summary>
    public const int MaxTrackedValues = 40;

    /// <summary>How many observations had a value of this kind at this position.</summary>
    public Dictionary<JsonValueKind, int> Kinds { get; } = [];

    /// <summary>How many documents reached this node at all — the denominator for optionality.</summary>
    public int Observations { get; private set; }

    /// <summary>Child shapes for an object, keyed by property name, in first-seen order.</summary>
    public Dictionary<string, NodeShape> Properties { get; } = [];

    /// <summary>Property names in the order the documents present them, so generated bodies read the same.</summary>
    public List<string> PropertyOrder { get; } = [];

    /// <summary>The shape of an array's elements, merged across every element seen.</summary>
    public NodeShape? Element { get; private set; }

    public int MinLength { get; private set; } = int.MaxValue;
    public int MaxLength { get; private set; }

    /// <summary>Distinct scalar values observed, capped at <see cref="MaxTrackedValues"/>.</summary>
    public List<JsonNode> Values { get; } = [];

    /// <summary>True once more distinct values were seen than are worth tracking — i.e. this is free-form.</summary>
    public bool Saturated { get; private set; }

    public double? Min { get; private set; }
    public double? Max { get; private set; }

    /// <summary>False as soon as a non-integral number turns up, so generated numbers keep the same flavour.</summary>
    public bool AllIntegral { get; private set; } = true;

    public DateTimeOffset? MinDate { get; private set; }
    public DateTimeOffset? MaxDate { get; private set; }

    /// <summary>The format of the first date seen, so generated dates are written back the same way.</summary>
    public string? DateFormat { get; private set; }

    public bool AllGuids { get; private set; } = true;
    bool sawString;

    /// <summary>The kind that best describes this node — the one seen most often, ignoring nulls.</summary>
    public JsonValueKind DominantKind
    {
        get
        {
            var ranked = this.Kinds
                .Where(k => k.Key is not (JsonValueKind.Null or JsonValueKind.Undefined))
                .OrderByDescending(k => k.Value)
                .ToList();

            return ranked.Count > 0 ? ranked[0].Key : JsonValueKind.Null;
        }
    }

    public bool IsObject => this.DominantKind == JsonValueKind.Object;
    public bool IsArray => this.DominantKind == JsonValueKind.Array;

    /// <summary>True when the values form a small closed set — a status, a tier, a category.</summary>
    public bool IsCategorical => !this.Saturated && this.Values.Count > 1;

    /// <summary>True when every string seen parses as a GUID, so generated ones should be GUIDs too.</summary>
    public bool IsGuid => this.sawString && this.AllGuids;

    /// <summary>True when every string seen parses as a date.</summary>
    public bool IsDate => this.MinDate is not null;

    /// <summary>How often this node was present, relative to its parent's observations.</summary>
    public double PresenceIn(int parentObservations)
        => parentObservations == 0 ? 0 : Math.Min(1, this.Observations / (double)parentObservations);

    public void Observe(JsonNode? node)
    {
        this.Observations++;
        var kind = node?.GetValueKind() ?? JsonValueKind.Null;
        this.Kinds[kind] = this.Kinds.GetValueOrDefault(kind) + 1;

        switch (node)
        {
            case JsonObject obj:
                foreach (var (key, value) in obj)
                {
                    if (!this.Properties.TryGetValue(key, out var child))
                    {
                        child = new NodeShape();
                        this.Properties[key] = child;
                        this.PropertyOrder.Add(key);
                    }

                    child.Observe(value);
                }
                break;

            case JsonArray array:
                this.MinLength = Math.Min(this.MinLength, array.Count);
                this.MaxLength = Math.Max(this.MaxLength, array.Count);
                this.Element ??= new NodeShape();
                foreach (var element in array)
                    this.Element.Observe(element);
                break;

            case JsonValue value:
                this.ObserveScalar(value, kind);
                break;
        }
    }

    void ObserveScalar(JsonValue value, JsonValueKind kind)
    {
        this.Track(value);

        if (kind == JsonValueKind.Number && value.TryGetValue<double>(out var number))
        {
            this.Min = this.Min is { } min ? Math.Min(min, number) : number;
            this.Max = this.Max is { } max ? Math.Max(max, number) : number;
            if (number != Math.Floor(number))
                this.AllIntegral = false;
        }

        if (kind != JsonValueKind.String || !value.TryGetValue<string>(out var text))
            return;

        this.sawString = true;
        if (!Guid.TryParse(text, out _))
            this.AllGuids = false;

        if (DateTimeOffset.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var date))
        {
            this.MinDate = this.MinDate is { } lo ? (date < lo ? date : lo) : date;
            this.MaxDate = this.MaxDate is { } hi ? (date > hi ? date : hi) : date;
            this.DateFormat ??= DetectDateFormat(text);
        }
        else
        {
            // One unparseable string is enough: a field is only a date field if all of it is.
            this.MinDate = null;
            this.MaxDate = null;
        }
    }

    void Track(JsonNode value)
    {
        if (this.Saturated)
            return;

        var text = value.ToJsonString();
        if (this.Values.Any(v => v.ToJsonString() == text))
            return;

        if (this.Values.Count >= MaxTrackedValues)
        {
            this.Saturated = true;
            return;
        }

        this.Values.Add(value.DeepClone());
    }

    /// <summary>
    /// Recovers the shape of a date string so generated ones match. Only the three forms that
    /// actually turn up in stored JSON are distinguished; anything else round-trips as ISO 8601.
    /// </summary>
    static string DetectDateFormat(string text) => text.Length switch
    {
        10 => "yyyy-MM-dd",
        <= 19 => "yyyy-MM-ddTHH:mm:ss",
        _ => text.EndsWith('Z') ? "yyyy-MM-ddTHH:mm:ss.fffZ" : "O"
    };
}

/// <summary>A profiled type: the shape of its documents plus how many were read to find it.</summary>
public sealed record DocumentShape(NodeShape Root, int Samples, string IdProperty)
{
    /// <summary>The top-level fields, for the "here is what I found" table.</summary>
    public IReadOnlyList<ShapeField> Fields =>
    [
        .. this.Root.PropertyOrder.Select(name =>
        {
            var shape = this.Root.Properties[name];
            return new ShapeField(name, shape, shape.PresenceIn(this.Root.Observations));
        })
    ];
}

/// <summary>One top-level field, described for display.</summary>
public sealed record ShapeField(string Name, NodeShape Shape, double Presence)
{
    public int PercentPresent => (int)Math.Round(this.Presence * 100);

    /// <summary>How the generator will treat this field — the honest summary of what it inferred.</summary>
    public string Strategy => this.Shape switch
    {
        { IsObject: true } => "nested object",
        { IsArray: true } => $"array of {this.Shape.MaxLength}",
        { IsGuid: true } => "new guid",
        { IsDate: true } => this.Shape.MinDate is { } lo && this.Shape.MaxDate is { } hi && lo != hi
            ? $"date {lo:yyyy-MM-dd} … {hi:yyyy-MM-dd}"
            : "date",
        { DominantKind: JsonValueKind.Number } => this.Shape is { Min: { } lo, Max: { } hi } && lo != hi
            ? $"number {Fmt(lo)} … {Fmt(hi)}"
            : "number",
        { DominantKind: JsonValueKind.True or JsonValueKind.False } => "true / false",
        { IsCategorical: true } => $"one of {this.Shape.Values.Count}",
        { Saturated: true } => "free text (sampled)",
        _ => "constant"
    };

    static string Fmt(double value)
        => value == Math.Floor(value)
            ? ((long)value).ToString(CultureInfo.InvariantCulture)
            : value.ToString("0.##", CultureInfo.InvariantCulture);
}

/// <summary>Reads a set of document bodies into a <see cref="DocumentShape"/>.</summary>
public static class DocumentShapeProfiler
{
    public static DocumentShape Profile(IEnumerable<string> bodies, string idProperty = "id")
    {
        var root = new NodeShape();
        var samples = 0;
        string? seenIdProperty = null;

        foreach (var body in bodies)
        {
            JsonNode? node;
            try
            {
                node = JsonNode.Parse(body);
            }
            catch (JsonException)
            {
                continue;
            }

            if (node is not JsonObject obj)
                continue;

            samples++;
            root.Observe(obj);

            // Match whichever casing the documents actually use, as the rest of the tool does.
            seenIdProperty ??= obj.ContainsKey("Id") ? "Id" : obj.ContainsKey("id") ? "id" : null;
        }

        return new DocumentShape(root, samples, seenIdProperty ?? idProperty);
    }
}
