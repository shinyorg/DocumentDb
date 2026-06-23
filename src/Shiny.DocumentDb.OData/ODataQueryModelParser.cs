namespace Shiny.DocumentDb.OData;

/// <summary>
/// Builds an <see cref="ODataQueryModel"/> from raw OData system query-option strings
/// (<c>$filter</c>, <c>$orderby</c>, <c>$top</c>, <c>$skip</c>, <c>$select</c>, <c>$count</c>,
/// <c>$expand</c>). This is the engine's dependency-free front-end; the ASP.NET host instead converts
/// the Microsoft parser result into the same <see cref="ODataQueryModel"/>.
/// </summary>
public static class ODataQueryModelParser
{
    /// <summary>
    /// Parses individual option strings (each without the leading <c>$name=</c>). Any may be
    /// <c>null</c>/empty.
    /// </summary>
    public static ODataQueryModel Parse(
        string? filter = null,
        string? orderby = null,
        int? top = null,
        int? skip = null,
        string? select = null,
        bool count = false,
        string? expand = null)
    {
        var model = new ODataQueryModel
        {
            Top = top,
            Skip = skip,
            Count = count,
            HasExpand = !string.IsNullOrWhiteSpace(expand)
        };

        if (!string.IsNullOrWhiteSpace(filter))
            model.Filter = ODataFilterParser.Parse(filter);

        if (!string.IsNullOrWhiteSpace(orderby))
            model.OrderBy = ParseOrderBy(orderby);

        if (!string.IsNullOrWhiteSpace(select))
            model.Select = select.Trim();

        return model;
    }

    /// <summary>
    /// Parses a raw OData query string (e.g. <c>$filter=Country eq 'CA'&amp;$top=20&amp;$count=true</c>),
    /// with or without a leading <c>?</c>.
    /// </summary>
    public static ODataQueryModel ParseQueryString(string queryString)
    {
        ArgumentNullException.ThrowIfNull(queryString);
        var q = queryString.TrimStart('?');

        string? filter = null, orderby = null, select = null, expand = null;
        int? top = null, skip = null;
        var count = false;

        foreach (var pair in q.Split('&', StringSplitOptions.RemoveEmptyEntries))
        {
            var idx = pair.IndexOf('=');
            var key = (idx < 0 ? pair : pair[..idx]).Trim();
            var rawValue = idx < 0 ? "" : pair[(idx + 1)..];
            var value = Uri.UnescapeDataString(rawValue.Replace('+', ' '));

            switch (key.ToLowerInvariant())
            {
                case "$filter": filter = value; break;
                case "$orderby": orderby = value; break;
                case "$select": select = value; break;
                case "$expand": expand = value; break;
                case "$top": top = int.Parse(value); break;
                case "$skip": skip = int.Parse(value); break;
                case "$count": count = value.Equals("true", StringComparison.OrdinalIgnoreCase); break;
            }
        }

        return Parse(filter, orderby, top, skip, select, count, expand);
    }

    static List<ODataOrderByItem> ParseOrderBy(string orderby)
    {
        var items = new List<ODataOrderByItem>();
        foreach (var term in orderby.Split(',', StringSplitOptions.RemoveEmptyEntries))
        {
            var parts = term.Trim().Split(' ', StringSplitOptions.RemoveEmptyEntries);
            var path = parts[0].Trim();
            var descending = parts.Length > 1 && parts[1].Equals("desc", StringComparison.OrdinalIgnoreCase);
            items.Add(new ODataOrderByItem(path, descending));
        }
        return items;
    }
}
