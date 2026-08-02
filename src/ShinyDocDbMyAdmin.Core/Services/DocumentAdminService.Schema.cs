using System.Text.Json;
using System.Text.Json.Nodes;
using ShinyDocDbMyAdmin.Models;

namespace ShinyDocDbMyAdmin.Services;

public sealed partial class DocumentAdminService
{
    /// <summary>How many documents to read when working out what a type looks like.</summary>
    public const int SchemaSampleSize = 200;

    /// <summary>
    /// Derives a shape for a type by sampling documents. The store is schema-free, so this is a
    /// description of what is actually there, not a contract - a field present in 40% of the sample
    /// is reported as exactly that rather than being hidden or promoted.
    /// </summary>
    public async Task<InferredSchema> InferSchema(
        string profileId,
        string table,
        string typeName,
        int sampleSize = SchemaSampleSize,
        CancellationToken ct = default)
    {
        var bodies = await this.SampleBodies(profileId, table, typeName, sampleSize, ct);

        var accumulators = new Dictionary<string, FieldAccumulator>(StringComparer.Ordinal);
        var order = new List<string>();
        var parsed = 0;

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

            parsed++;
            Walk(obj, prefix: "", accumulators, order);
        }

        var fields = order
            .Select(path => accumulators[path])
            .Select(a => new InferredField(a.Path, a.TypeSummary, a.Occurrences, parsed, a.Example))
            .ToList();

        return new InferredSchema(typeName, parsed, fields);
    }

    /// <summary>
    /// Reads a sample of raw document bodies, newest first - a type whose shape changed over time
    /// should be described by what it looks like now. Shared by schema inference and geometry discovery.
    /// </summary>
    public async Task<IReadOnlyList<string>> SampleBodies(
        string profileId,
        string table,
        string typeName,
        int sampleSize = SchemaSampleSize,
        CancellationToken ct = default)
    {
        var connection = await this.Connect(profileId, ct);
        var provider = connection.Provider;
        var quoted = provider.QuoteTable(Ado.SafeIdentifier(table));

        return await connection.Execute(async (db, token) =>
        {
            var sql = $"SELECT Data FROM {quoted} WHERE TypeName = @typeName ORDER BY UpdatedAt DESC, Id ASC " +
                      provider.BuildPaginationClause(0, sampleSize);

            await using var cmd = Ado.Command(db, sql);
            Ado.Bind(cmd, "@typeName", typeName);

            await using var reader = await cmd.ExecuteReaderAsync(token);
            var results = new List<string>();
            while (await reader.ReadAsync(token))
                results.Add(Ado.Text(reader, 0));

            return (IReadOnlyList<string>)results;
        }, ct);
    }

    /// <summary>
    /// The columns the browse grid shows by default: scalar fields present in most documents, in the
    /// order they appear in the documents themselves, capped so a wide type stays readable.
    /// </summary>
    public static IReadOnlyList<string> DefaultColumns(InferredSchema schema, int max = 6)
        => [.. schema.Fields
            .Where(f => !f.Types.Contains("object") && !f.Types.Contains("array"))
            .Where(f => !f.Path.Equals("id", StringComparison.OrdinalIgnoreCase))
            .OrderByDescending(f => f.Occurrences)
            .Take(max)
            .Select(f => f.Path)];

    /// <summary>The paths the quick-search box covers - string fields, plus the envelope Id.</summary>
    public static IReadOnlyList<string> SearchableColumns(InferredSchema schema, int max = 8)
        => [
            "Id",
            .. schema.Fields
                .Where(f => f.Types.Contains("string"))
                .OrderByDescending(f => f.Occurrences)
                .Take(max)
                .Select(f => f.Path)
        ];

    static void Walk(JsonObject obj, string prefix, Dictionary<string, FieldAccumulator> accumulators, List<string> order)
    {
        foreach (var (key, value) in obj)
        {
            var path = prefix.Length == 0 ? key : $"{prefix}.{key}";

            if (!accumulators.TryGetValue(path, out var accumulator))
            {
                accumulator = new FieldAccumulator(path);
                accumulators[path] = accumulator;
                order.Add(path);
            }

            accumulator.Observe(value);

            // One level of nesting is genuinely useful (shippingAddress.city); deeper turns the
            // structure tab into noise, and the raw JSON view is there for the rest.
            if (value is JsonObject nested && prefix.Length == 0)
                Walk(nested, path, accumulators, order);
        }
    }

    sealed class FieldAccumulator(string path)
    {
        readonly SortedSet<string> types = new(StringComparer.Ordinal);

        public string Path { get; } = path;
        public int Occurrences { get; private set; }
        public string? Example { get; private set; }

        public string TypeSummary => this.types.Count == 0 ? "null" : string.Join(" | ", this.types);

        public void Observe(JsonNode? value)
        {
            this.Occurrences++;
            this.types.Add(Describe(value));

            if (this.Example is null && value is not null and not JsonObject and not JsonArray)
            {
                var text = value.ToJsonString();
                this.Example = text.Length > 60 ? text[..57] + "..." : text;
            }
        }

        static string Describe(JsonNode? value) => value switch
        {
            null => "null",
            JsonArray => "array",
            JsonObject => "object",
            JsonValue v => v.GetValueKind() switch
            {
                JsonValueKind.String => "string",
                JsonValueKind.Number => "number",
                JsonValueKind.True or JsonValueKind.False => "bool",
                JsonValueKind.Null => "null",
                _ => "value"
            },
            _ => "value"
        };
    }
}
