namespace Sample.McpServer;

/// <summary>The landing page: how to point a client at this server, and what to ask it once you have.</summary>
static class Explorer
{
    public const string Html =
        """
        <!doctype html>
        <meta charset="utf-8">
        <title>Shiny.DocumentDb — MCP sample</title>
        <style>
          body { font: 15px/1.6 system-ui, sans-serif; max-width: 62rem; margin: 3rem auto; padding: 0 1rem; }
          code, pre { background: #f4f4f5; border-radius: .3rem; }
          code { padding: .1rem .3rem; }
          pre { padding: .8rem 1rem; overflow-x: auto; }
          li { margin: .3rem 0; }
          table { border-collapse: collapse; margin: 1rem 0; }
          th, td { border: 1px solid #e4e4e7; padding: .4rem .7rem; text-align: left; }
        </style>
        <h1>Shiny.DocumentDb — MCP server sample</h1>
        <p>
          The endpoint is <code>http://localhost:5097/mcp</code> (Streamable HTTP). Same data as
          <code>samples/Sample.ODataApi</code> and <code>samples/Sample.RestApi</code>.
        </p>

        <h2>Point a client at it</h2>
        <p><b>Claude Code</b> — <code>.mcp.json</code> in your project:</p>
        <pre><code>{
          "mcpServers": {
            "documentdb": {
              "type": "http",
              "url": "http://localhost:5097/mcp",
              "headers": { "X-Country": "CA" }
            }
          }
        }</code></pre>

        <p><b>Claude Desktop</b> — <code>claude_desktop_config.json</code>, same shape.</p>

        <p><b>No server at all</b> — the stdio tool needs no host and no compiled document classes:</p>
        <pre><code>dotnet tool install -g ShinyDocDbMcp
        shiny-documentdb-mcp --provider sqlite --connection "Data Source=$TMPDIR/sample-mcp-server.db"</code></pre>
        <p>
          It discovers what to expose from the stored <code>TypeName</code> discriminators. What it cannot do is
          a per-caller scope — a config file cannot express a lambda, and stdio has no caller identity. That is
          what this host is for.
        </p>

        <h2>Things to ask</h2>
        <ul>
          <li>“What document types are available?” → reads <code>documentdb://types</code></li>
          <li>“Show me the shape of an order” → <code>documentdb://types/order/sample</code></li>
          <li>“How many orders are still pending?” → <code>order_count</code></li>
          <li>“Which customers in Canada are over 40?” → <code>customer_query</code></li>
          <li>“What is the average order total by status?” → <code>order_aggregate</code></li>
          <li>“Delete the cancelled orders” → <b>refused</b>: no write tool exists</li>
        </ul>

        <h2>What the scope does</h2>
        <p>
          Orders carry <code>.Where&lt;ICallerContext&gt;((caller, _) =&gt; o =&gt; o.Country == caller.Country)</code>,
          resolved on every tool call. Change the header and the same tool returns different rows — the model is
          never told why, and cannot ask for more.
        </p>
        <table>
          <tr><th>Header</th><th>What the model can reach</th></tr>
          <tr><td><code>X-Country: CA</code></td><td>Canadian orders only</td></tr>
          <tr><td><code>X-Country: US</code></td><td>US orders only</td></tr>
        </table>
        <p>Customers are deliberately <em>not</em> scoped, so you can see the difference in one session.</p>

        <h2>What it will not do</h2>
        <ul>
          <li><b>No writes.</b> Read-only is the default; a write tool needs <code>mcp.AllowWrites()</code>
              <em>and</em> a per-type write capability.</li>
          <li><b>No raw SQL.</b> Not even read-only, by design.</li>
          <li><b>No schema changes.</b></li>
          <li><b>No <code>email</code>.</b> <code>IgnoreProperties(c =&gt; c.Email)</code> keeps it out of the
              schema and out of every result.</li>
          <li><b>No pulling the table into context.</b> <code>MaxPageSize</code> caps every query.</li>
        </ul>

        <p>Full documentation: <a href="https://shinylib.net/documentdb/mcp">shinylib.net/documentdb/mcp</a></p>
        """;
}
