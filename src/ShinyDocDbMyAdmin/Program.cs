using ShinyDocDbMyAdmin.Components;
using ShinyDocDbMyAdmin.Services;

// ContentRootPath, not the current directory: this ships as a .NET tool, so it is launched from
// wherever the caller happens to be standing. Left at the default, wwwroot never resolves and every
// static asset is served as an empty 200.
var builder = WebApplication.CreateBuilder(new WebApplicationOptions
{
    Args = args,
    ContentRootPath = AppContext.BaseDirectory
});

// Only one SQLitePCLRaw bundle ships in the output (see the csproj note): e_sqlcipher, which opens
// both plain and encrypted SQLite files. Initialising it here makes that choice visible rather than
// leaving it to whichever bundle Microsoft.Data.Sqlite happens to find first.
SQLitePCL.Batteries_V2.Init();

// Demo mode is settled before anything is registered, because it decides what gets registered at
// all - and because the sample has to exist on disk before ProvidedConnections reads the
// configuration that points at it.
var demoMode = DemoMode.IsEnabledIn(builder.Configuration);
if (demoMode)
{
    using var startup = LoggerFactory.Create(logging => logging.AddConfiguration(builder.Configuration.GetSection("Logging")).AddConsole());
    var log = startup.CreateLogger("ShinyDocDbMyAdmin.Demo");

    var dataDirectory = new AppPaths(builder.Configuration).DataDirectory;
    var databasePath = Path.Combine(dataDirectory, DemoMode.DatabaseFileName);
    DemoDatabaseBuilder.EnsureCreated(databasePath, log);

    // Published through the existing host-provided-connection contract rather than a special case:
    // those are already un-editable and un-deletable in the UI, which is most of what demo mode
    // needs the connection to be. Everything downstream treats it as an ordinary connection.
    builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
    {
        [$"ConnectionStrings:{DemoMode.ConnectionName}"] = $"Data Source={databasePath}",
        [$"Shiny:DocumentDb:{DemoMode.ConnectionName}:Provider"] = "Sqlite",

        // Read-only twice over: this makes the UI say so rather than offering buttons that fail,
        // and the mount underneath it should be read-only too.
        ["ShinyDocDbMyAdmin:ReadOnly"] = "true",

        // DisableAi exists for exactly this deployment. Forcing it here means a demo instance cannot
        // be one forgotten setting away from inviting a stranger to paste their own API key in.
        [AiAvailability.DisableKey] = "true"
    });
}

builder.Services.AddRazorComponents()
    .AddInteractiveServerComponents();

builder.Services.Configure<Microsoft.AspNetCore.SignalR.HubOptions>(options =>
{
    // Uploads arrive over the circuit; the default 32 KB ceiling makes a large database file crawl.
    options.MaximumReceiveMessageSize = 1024 * 1024;
});

builder.Services.AddSingleton<DemoMode>();
builder.Services.AddSingleton<AppPaths>();
builder.Services.AddSingleton<SecretProtector>();
builder.Services.AddSingleton<ProvidedConnections>();
builder.Services.AddSingleton<ProfileStore>();
builder.Services.AddSingleton<ConnectionManager>();
builder.Services.AddSingleton<DocumentAdminService>();
builder.Services.AddSingleton<EncryptionKeyRing>();
builder.Services.AddSingleton<ImportExportService>();
builder.Services.AddSingleton<ConnectionTransferService>();
builder.Services.AddSingleton<DatabaseUploadService>();

// Per-circuit, so one browser tab's refresh notifications do not fan out to every other session.
builder.Services.AddScoped<UiEvents>();

// The assistant. AiAvailability is always registered - the pages need something to ask - but when
// the deployment has switched the feature off, nothing behind it is. A demo instance then cannot
// serve an assistant even if a page forgets to check: there is no client factory to resolve.
builder.Services.AddSingleton<AiAvailability>();
if (!builder.Configuration.GetValue(AiAvailability.DisableKey, false))
{
    builder.Services.AddSingleton<AiClientFactory>();
    builder.Services.AddSingleton<AiToolSurface>();

    // Per-circuit: the transcript is one browser tab's conversation and never leaves memory.
    builder.Services.AddScoped<AiChatSessionProvider>();
}

var app = builder.Build();

if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error", createScopeForErrors: true);
    app.UseHsts();
}

app.UseStatusCodePagesWithReExecute("/not-found", createScopeForStatusCodePages: true);
app.UseHttpsRedirection();

app.MapStaticAssets();
app.MapExportEndpoints();
app.MapBlobEndpoints();

app.MapRazorComponents<App>()
    .AddInteractiveServerRenderMode()
    .DisableAntiforgery();

app.Run();
