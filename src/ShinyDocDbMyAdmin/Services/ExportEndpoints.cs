using System.Text.Json;
using ShinyDocDbMyAdmin.Models;

namespace ShinyDocDbMyAdmin.Services;

/// <summary>
/// Exports are plain HTTP rather than Blazor interactions: the response streams straight from the
/// database to the browser, so a million-document export never lands in the circuit's memory.
/// </summary>
public static class ExportEndpoints
{
    public static void MapExportEndpoints(this WebApplication app)
    {
        app.MapGet("/export/{profileId}/{table}/{typeName}", async (
            string profileId,
            string table,
            string typeName,
            HttpContext context,
            ImportExportService transfers,
            ProfileStore profiles,
            CancellationToken ct) =>
        {
            if (await profiles.Get(profileId, ct) is null)
                return Results.NotFound();

            var format = Enum.TryParse<ExportFormat>(context.Request.Query["format"], ignoreCase: true, out var parsed)
                ? parsed
                : ExportFormat.Json;

            var query = new BrowseQuery
            {
                Sort = new BrowseSort("Id", false, true),
                Filters = ParseFilters(context.Request.Query["filter"]),
                Search = context.Request.Query["q"],
                SearchPaths = ParsePaths(context.Request.Query["qp"])
            };

            var fileName = $"{Sanitise(typeName)}.{ImportExportService.FileExtension(format)}";
            context.Response.ContentType = ImportExportService.ContentType(format);
            context.Response.Headers.ContentDisposition = $"attachment; filename=\"{fileName}\"";

            await transfers.Export(profileId, table, typeName, format, query, context.Response.Body, ct);
            return Results.Empty;
        });
    }

    /// <summary>Filters travel as a JSON array so the export link mirrors whatever the Browse tab is showing.</summary>
    static IReadOnlyList<BrowseFilter> ParseFilters(string? encoded)
    {
        if (string.IsNullOrWhiteSpace(encoded))
            return [];

        try
        {
            using var document = JsonDocument.Parse(encoded);
            var filters = new List<BrowseFilter>();
            foreach (var element in document.RootElement.EnumerateArray())
            {
                var path = element.GetProperty("p").GetString();
                if (string.IsNullOrWhiteSpace(path))
                    continue;

                var op = Enum.TryParse<FilterOperator>(element.GetProperty("o").GetString(), out var parsed)
                    ? parsed
                    : FilterOperator.Equals;

                filters.Add(new BrowseFilter(path, op, element.TryGetProperty("v", out var v) ? v.GetString() ?? "" : ""));
            }
            return filters;
        }
        catch (JsonException)
        {
            // A hand-mangled export URL should download an unfiltered file, not throw a 500.
            return [];
        }
    }

    static IReadOnlyList<string> ParsePaths(string? encoded)
        => string.IsNullOrWhiteSpace(encoded)
            ? []
            : encoded.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

    static string Sanitise(string name)
    {
        var cleaned = new string([.. name.Select(c => char.IsLetterOrDigit(c) || c is '-' or '_' or '.' ? c : '_')]);
        return string.IsNullOrWhiteSpace(cleaned) ? "export" : cleaned;
    }
}
