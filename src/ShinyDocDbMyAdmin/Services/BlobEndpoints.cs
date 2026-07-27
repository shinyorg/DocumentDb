using ShinyDocDbMyAdmin.Models;

namespace ShinyDocDbMyAdmin.Services;

/// <summary>
/// Serves blob payloads over plain HTTP, so a download streams to the browser instead of crossing the
/// Blazor circuit as base64.
/// </summary>
/// <remarks>
/// Blob bytes come from whoever wrote the document, so serving them from our own origin is a real XSS
/// surface. Two rules keep it closed: every response carries <c>nosniff</c> and a
/// <c>default-src 'none'</c> CSP, and anything not on the short raster-image allowlist is sent as an
/// attachment rather than rendered. SVG is excluded from inline display on purpose - it is a document
/// format that can carry script, not merely an image.
/// </remarks>
public static class BlobEndpoints
{
    public static void MapBlobEndpoints(this WebApplication app)
    {
        app.MapGet("/blob/{profileId}/{table}/{typeName}/{documentId}/{blobKey}", async (
            string profileId,
            string table,
            string typeName,
            string documentId,
            string blobKey,
            HttpContext context,
            DocumentAdminService admin,
            CancellationToken ct) =>
        {
            BlobPayload? payload;
            try
            {
                payload = await admin.ReadBlob(profileId, table, typeName, documentId, blobKey, ct);
            }
            catch (Exception ex) when (ex is InvalidOperationException or ArgumentException)
            {
                return Results.NotFound();
            }

            if (payload is null)
                return Results.NotFound();

            var wantsInline = context.Request.Query["inline"] == "1";
            var inlineAllowed = wantsInline && payload.Info.Kind == BlobKind.Image;

            context.Response.Headers.XContentTypeOptions = "nosniff";
            context.Response.Headers.ContentSecurityPolicy = "default-src 'none'; sandbox";

            var fileName = SafeFileName(payload.Info.FileName ?? blobKey);
            context.Response.Headers.ContentDisposition = inlineAllowed
                ? $"inline; filename=\"{fileName}\""
                : $"attachment; filename=\"{fileName}\"";

            // Only an allowlisted type is echoed back; everything else is an opaque download, so a
            // stored text/html payload can never be handed to the browser as a page.
            var contentType = inlineAllowed ? payload.Info.ContentType! : "application/octet-stream";
            return Results.Bytes(payload.Data, contentType);
        });
    }

    /// <summary>Reduces a stored name to a bare, quote-free basename fit for a Content-Disposition header.</summary>
    static string SafeFileName(string name)
    {
        var basename = Path.GetFileName(name);
        if (string.IsNullOrWhiteSpace(basename))
            basename = "blob";

        var cleaned = new string([.. basename.Select(c =>
            char.IsLetterOrDigit(c) || c is '-' or '_' or '.' or ' ' ? c : '_')]);

        return cleaned.Length > 120 ? cleaned[..120] : cleaned;
    }
}
