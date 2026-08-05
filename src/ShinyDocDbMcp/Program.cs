using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Shiny.DocumentDb;
using Shiny.DocumentDb.Extensions.AI;
using Shiny.DocumentDb.Mcp;
using ShinyDocDbMcp;
using ShinyDocDbMyAdmin.Providers;
using ShinyDocDbMyAdmin.Services;

const string Usage =
    """
    shiny-documentdb-mcp — an MCP server over a Shiny.DocumentDb store (stdio transport).

      shiny-documentdb-mcp --profile <name>                        a saved ShinyDocDbMyAdmin connection
      shiny-documentdb-mcp --provider <kind> --connection <string> [--password <pw>]
      shiny-documentdb-mcp --config ./documentdb-mcp.json          collections, scopes, capabilities

    Options
      --table <name>            documents table (default: documents)
      --types A,B               expose only these TypeName discriminators (default: discover all)
      --max-page-size <n>       cap on rows per query (default: 100)
      --allow-writes            unlock insert/update/delete — off by default, and each collection
                                must also opt in. A scoped collection can never be writable.
      --help

    Read-only unless --allow-writes is passed. Nothing not registered is visible.
    """;

McpToolConfig config;
try
{
    config = McpToolConfig.Parse(args);
}
catch (UsageException ex)
{
    // stdout is the MCP transport — usage goes to stderr so it can never corrupt a session.
    if (!string.IsNullOrEmpty(ex.Message))
        await Console.Error.WriteLineAsync(ex.Message);
    await Console.Error.WriteLineAsync(Usage);
    return string.IsNullOrEmpty(ex.Message) ? 0 : 2;
}

var builder = Host.CreateApplicationBuilder(args);
builder.Configuration.AddEnvironmentVariables();

// Every log line goes to stderr, for the same reason.
builder.Logging.ClearProviders();
builder.Logging.AddConsole(o => o.LogToStandardErrorThreshold = LogLevel.Trace);

var provider = await ResolveProvider(builder.Configuration, config);
var typeNames = config.Collections.Count > 0
    ? config.Collections.Select(c => c.Name).ToList()
    : (await TypeDiscovery.ListTypeNames(provider, config.Table, CancellationToken.None)).ToList();

if (typeNames.Count == 0)
{
    await Console.Error.WriteLineAsync(
        $"No document types found in table '{config.Table}'. Nothing to expose — pass --types or --config to " +
        "declare them explicitly, or check --table.");
    return 3;
}

builder.Services.AddDocumentStore(o =>
{
    o.DatabaseProvider = provider;
    o.TableName = config.Table;
    o.SkipTableInitialization = true;   // an MCP server never creates schema; it reads what is already there
});

builder.Services
    .AddDocumentDbMcpServer(mcp =>
    {
        if (config.AllowWrites)
            mcp.AllowWrites();

        mcp.ExposeResources();
        mcp.ExposePrompts();

        foreach (var name in typeNames)
        {
            var declared = config.Collections.FirstOrDefault(
                c => string.Equals(c.Name, name, StringComparison.OrdinalIgnoreCase));

            // A scope filter and a write capability cannot coexist on the schema-free lane (there is no
            // evaluator for a raw body), so a scoped collection stays read-only whatever --allow-writes says.
            var scoped = !string.IsNullOrWhiteSpace(declared?.Where);
            var capabilities = config.AllowWrites && !scoped
                ? DocumentAICapabilities.All
                : DocumentAICapabilities.ReadOnly;

            mcp.AddCollection(name, capabilities: capabilities, configure: c =>
            {
                c.IdProperty(declared?.IdProperty ?? "id");
                c.MaxPageSize(config.MaxPageSize);

                if (!string.IsNullOrWhiteSpace(declared?.Description))
                    c.Description(declared!.Description!);

                if (declared is { Fields.Count: > 0 })
                {
                    foreach (var field in declared.Fields)
                        c.Field(field.Path, ParseFieldType(field.Type), field.Description);
                }
                else
                {
                    // Discovered, not declared: the model may name any path. Declare fields in --config when
                    // the collection holds anything you would rather it did not go looking through.
                    c.AllowAnyField();
                }

                if (scoped)
                    c.Where(declared!.Where!);
            });
        }
    })
    .WithStdioServerTransport();

var host = builder.Build();
await host.RunAsync();
return 0;

static DocumentFieldType ParseFieldType(string type)
    => Enum.TryParse<DocumentFieldType>(type, ignoreCase: true, out var parsed)
        ? parsed
        : throw new UsageException(
            $"Unknown field type '{type}'. Known: {string.Join(", ", Enum.GetNames<DocumentFieldType>())}.");

static async Task<IDatabaseProvider> ResolveProvider(IConfiguration configuration, McpToolConfig config)
{
    if (config.Profile is null)
        return ProviderCatalog.Create(config.Provider!.Value, config.ConnectionString!, config.Password);

    // Saved connections come from the admin tool's own store, decrypted by its own SecretProtector — this
    // package stores no credentials and never takes them as tool arguments.
    using var loggerFactory = LoggerFactory.Create(b => b.AddConsole(o => o.LogToStandardErrorThreshold = LogLevel.Trace));
    var paths = new AppPaths(configuration);
    var protector = new SecretProtector(configuration, paths, loggerFactory.CreateLogger<SecretProtector>());
    var provided = new ProvidedConnections(configuration, loggerFactory.CreateLogger<ProvidedConnections>());
    var demo = new DemoMode(configuration, loggerFactory.CreateLogger<DemoMode>());
    var profiles = new ProfileStore(paths, protector, provided, demo);

    var all = await profiles.List();
    var match = all.FirstOrDefault(p =>
        string.Equals(p.Name, config.Profile, StringComparison.OrdinalIgnoreCase) ||
        string.Equals(p.Id, config.Profile, StringComparison.OrdinalIgnoreCase))
        ?? throw new UsageException(
            $"No connection profile named '{config.Profile}'. Known: {string.Join(", ", all.Select(p => p.Name))}.");

    var resolved = profiles.Resolve(match);

    // A profile marked read-only in the admin tool stays read-only here — the flag travels with the connection.
    if (resolved.ReadOnly)
        config.AllowWrites = false;

    return ProviderCatalog.Create(resolved.Provider, resolved.ConnectionString, resolved.Password, resolved.FilePath);
}
