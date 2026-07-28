using System.Text.Json.Nodes;
using ShinyDocDbMyAdmin.Models;
using Shiny.DocumentDb;

namespace ShinyDocDbMyAdmin.Services;

/// <summary>
/// Making more documents that look like the ones already there — for filling a dev database, giving
/// a query plan something to chew on, or reproducing a report of "it gets slow past 100k".
/// </summary>
public sealed partial class DocumentAdminService
{
    /// <summary>The most records one run will write. High enough to make an index matter, low enough to stay a click.</summary>
    public const int MaxGeneratedRecords = 100_000;

    /// <summary>How many generated records the preview shows before you commit.</summary>
    public const int GeneratePreviewCount = 5;

    /// <summary>Reads a type's documents and works out how to make more of them.</summary>
    public async Task<DocumentShape> AnalyzeShape(
        string profileId,
        string table,
        string typeName,
        int sampleSize = SchemaSampleSize,
        CancellationToken ct = default)
    {
        var bodies = await this.SampleBodies(profileId, table, typeName, sampleSize, ct);
        var shape = DocumentShapeProfiler.Profile(bodies);

        if (shape.Samples == 0)
            throw new InvalidOperationException(
                $"There are no '{typeName}' documents to learn from. Insert one first — the generator " +
                $"copies the shape of what is already there rather than inventing a schema.");

        return shape;
    }

    /// <summary>
    /// Generates records without writing them. The seed comes back so the commit can reproduce the
    /// same documents rather than a different set that merely looks similar.
    /// </summary>
    public static GenerationPreview Preview(DocumentShape shape, int count, int? seed = null)
    {
        var actual = Math.Clamp(count, 1, MaxGeneratedRecords);
        var chosen = seed ?? DocumentGenerator.NewSeed();

        var sample = new DocumentGenerator(shape, chosen)
            .Generate(Math.Min(actual, GeneratePreviewCount));

        return new GenerationPreview(shape, sample, actual, chosen);
    }

    /// <summary>
    /// Writes the generated records, reproducing exactly what the preview showed.
    /// </summary>
    /// <remarks>
    /// Goes through the schema-free JSON collection lane rather than this tool's per-document write
    /// path: one transaction for the whole batch instead of one each, which is the difference between
    /// a click and a coffee at 100k rows. The cost is that lane's documented limit — it maintains no
    /// sidecars — so a type with temporal history, vectors, spatial or blobs gets rows the database
    /// has, and those indexes do not. The caller is told which, rather than finding out later.
    /// </remarks>
    public async Task<GenerationResult> Commit(
        string profileId,
        string table,
        string typeName,
        GenerationPreview preview,
        CancellationToken ct = default)
    {
        var connection = await this.Connect(profileId, ct);
        connection.AssertWritable();

        var documents = new DocumentGenerator(preview.Shape, preview.Seed).Generate(preview.Count);

        using var store = await this.OpenStore(profileId, table, ct);
        var collection = store.Collection(typeName, preview.Shape.IdProperty);

        var written = 0;
        // Chunked so one enormous statement is never built, and so a failure part-way reports how
        // far it actually got rather than an all-or-nothing lie.
        foreach (var chunk in documents.Chunk(500))
        {
            var array = new JsonArray();
            foreach (var document in chunk)
                array.Add(document.DeepClone());

            written += await collection.Insert(array, ct);
        }

        this.InvalidateSchema(profileId);
        logger.LogInformation("Generated {Count} {Type} document(s) into {Table}", written, typeName, table);

        return new GenerationResult(written, await this.StaleSidecars(profileId, table, typeName, ct));
    }

    /// <summary>
    /// Which of the type's sidecars the generated rows are missing from, so the UI can say so and
    /// point at the repair rather than leaving a silently wrong index behind.
    /// </summary>
    async Task<IReadOnlyList<string>> StaleSidecars(string profileId, string table, string typeName, CancellationToken ct)
    {
        var stale = new List<string>();
        var connection = await this.Connect(profileId, ct);
        var provider = connection.Provider;
        var safeTable = Ado.SafeIdentifier(table);
        var tables = await this.ListTables(profileId, refresh: true, ct);

        bool Exists(string name) => tables.Any(t => t.Name.Equals(name, StringComparison.OrdinalIgnoreCase));

        // The history and spatial sidecars are per *table*, shared by every type in it, so their mere
        // existence says nothing about this type — one mapped type creates them for all of them.
        // Whether this type has ever had a row there is the question, and the only one the schema can
        // answer: neither mapping is recoverable without the CLR type. A type with no rows yet is
        // reported as having nothing stale, which is the right answer for an empty type and a wrong
        // one for a mapped type nobody has written to — an under-claim rather than a false alarm.
        var history = provider.HistoryTableName(safeTable);
        if (Exists(history) && await this.HasRowsForType(connection, provider.QuoteTable(history), "TypeName", typeName, ct))
            stale.Add("temporal history");

        // Vectors are already per type: the sidecar is named for it.
        if ((await this.DescribeVectorSidecar(profileId, table, typeName, refresh: true, ct)).Present)
            stale.Add("vector index");

        var spatial = $"{safeTable}_spatial";
        if (Exists(spatial) && await this.HasRowsForType(connection, provider.QuoteTable(spatial), "typeName", typeName, ct))
            stale.Add("spatial index");

        return stale;
    }

    async Task<bool> HasRowsForType(
        AdminConnection connection,
        string quotedTable,
        string column,
        string typeName,
        CancellationToken ct)
    {
        try
        {
            return await connection.Execute(async (db, token) =>
            {
                await using var cmd = Ado.Command(db, $"SELECT 1 FROM {quotedTable} WHERE {column} = @typeName");
                Ado.Bind(cmd, "@typeName", typeName);

                await using var reader = await cmd.ExecuteReaderAsync(token);
                return await reader.ReadAsync(token);
            }, ct);
        }
        catch (System.Data.Common.DbException ex)
        {
            // A sidecar shaped differently than expected is not a reason to fail a write that has
            // already succeeded; it just means this particular claim cannot be made.
            logger.LogDebug(ex, "Could not check {Table} for {Type}", quotedTable, typeName);
            return false;
        }
    }
}
