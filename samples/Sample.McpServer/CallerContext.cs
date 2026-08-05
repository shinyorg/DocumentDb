namespace Sample.McpServer;

/// <summary>
/// Who is calling, and therefore which rows they may see. A request-resolved scope filter resolves this out
/// of the tool call's own service provider, which on the HTTP transport is the request scope.
/// </summary>
public interface ICallerContext
{
    /// <summary>The country whose orders this caller may see.</summary>
    string Country { get; }
}

/// <summary>
/// The sample's stand-in for real authorization: the caller's country comes from an <c>X-Country</c> header,
/// so you can switch callers with <c>curl</c> and watch the same tool return different rows.
/// </summary>
/// <remarks>
/// A real deployment reads this off the authenticated principal (<c>http.User</c>), a tenant resolver, or a
/// permission service — never off a header the caller controls. The shape is identical; only the source line
/// changes.
/// </remarks>
public sealed class HeaderCallerContext(IHttpContextAccessor accessor) : ICallerContext
{
    public string Country
    {
        get
        {
            var header = accessor.HttpContext?.Request.Headers["X-Country"].ToString();

            // Defaulting rather than denying keeps the sample usable from a client that cannot set headers.
            // A real app does the opposite: no identity means DocumentScope-style deny-all, never "everything".
            return string.IsNullOrWhiteSpace(header) ? "CA" : header.ToUpperInvariant();
        }
    }
}
