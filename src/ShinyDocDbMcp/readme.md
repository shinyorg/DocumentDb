# ShinyDocDbMcp

An **MCP (Model Context Protocol) server** over a [Shiny.DocumentDb](https://shinylib.net/documentdb) store,
speaking stdio — point Claude Code, Claude Desktop, Copilot or any other MCP client at a database and let it
explore and query the data.

```bash
dotnet tool install -g ShinyDocDbMcp
```

```jsonc
// .mcp.json
{
  "mcpServers": {
    "documentdb": {
      "command": "shiny-documentdb-mcp",
      "args": ["--provider", "sqlite", "--connection", "Data Source=app.db"]
    }
  }
}
```

## Connecting

```bash
shiny-documentdb-mcp --profile prod-readonly                        # a saved ShinyDocDbMyAdmin connection
shiny-documentdb-mcp --provider sqlite --connection "Data Source=app.db"
shiny-documentdb-mcp --config ./documentdb-mcp.json                 # collections, scopes, capabilities
```

Saved profiles are the ones the admin tools already hold, decrypted by the same key. This tool stores no
credentials of its own and never accepts them as tool arguments.

## What the model can and cannot do

- **Read-only by default.** `--allow-writes` is one of *two* locks; the collection must opt in as well, and a
  collection with a scope filter can never be writable.
- **Page caps** (`--max-page-size`, default 100) so a model cannot pull a table into its context.
- **Scope filters** (`where` in the config file) are applied server-side and are invisible to the model.
- **No raw SQL tool**, at all, by design.
- **No schema mutation** — the admin tools own that, interactively.

Full documentation: <https://shinylib.net/documentdb/mcp>
