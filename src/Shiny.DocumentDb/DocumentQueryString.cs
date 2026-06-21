using System.Text;

namespace Shiny.DocumentDb;

/// <summary>
/// The provider query a configured <see cref="IDocumentQuery{T}"/> would execute, captured without
/// running it (see <see cref="IDocumentQuery{T}.ToQueryString"/>). For relational providers and Cosmos
/// <see cref="Sql"/> holds the parameterized SQL text and <see cref="Parameters"/> the bound values; for
/// MongoDB <see cref="Sql"/> holds the rendered BSON filter (or full find command) as JSON and
/// <see cref="Parameters"/> is empty (values are inlined in the BSON).
/// </summary>
public sealed class DocumentQueryString
{
    public DocumentQueryString(string sql, IReadOnlyDictionary<string, object?> parameters)
    {
        this.Sql = sql;
        this.Parameters = parameters;
    }

    /// <summary>
    /// The raw query text: SQL for relational/Cosmos providers, BSON JSON for MongoDB.
    /// </summary>
    public string Sql { get; }

    /// <summary>
    /// The parameter name → value map bound to <see cref="Sql"/>. Empty when the provider inlines
    /// values (MongoDB) or the query takes no parameters.
    /// </summary>
    public IReadOnlyDictionary<string, object?> Parameters { get; }

    /// <summary>
    /// Renders the query with each parameter's value declared as a leading comment line, then the
    /// query text — handy for logging and copy/paste.
    /// </summary>
    public override string ToString()
    {
        if (this.Parameters.Count == 0)
            return this.Sql;

        var sb = new StringBuilder();
        foreach (var kvp in this.Parameters)
            sb.Append("-- ").Append(kvp.Key).Append('=').Append(FormatValue(kvp.Value)).Append('\n');
        sb.Append(this.Sql);
        return sb.ToString();
    }

    static string FormatValue(object? value) => value switch
    {
        null => "NULL",
        string s => $"'{s}'",
        bool b => b ? "true" : "false",
        _ => value.ToString() ?? "NULL"
    };
}
