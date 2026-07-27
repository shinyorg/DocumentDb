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

builder.Services.AddRazorComponents()
    .AddInteractiveServerComponents();

builder.Services.Configure<Microsoft.AspNetCore.SignalR.HubOptions>(options =>
{
    // Uploads arrive over the circuit; the default 32 KB ceiling makes a large database file crawl.
    options.MaximumReceiveMessageSize = 1024 * 1024;
});

builder.Services.AddSingleton<AppPaths>();
builder.Services.AddSingleton<SecretProtector>();
builder.Services.AddSingleton<ProvidedConnections>();
builder.Services.AddSingleton<ProfileStore>();
builder.Services.AddSingleton<ConnectionManager>();
builder.Services.AddSingleton<DocumentAdminService>();
builder.Services.AddSingleton<ImportExportService>();
builder.Services.AddSingleton<DatabaseUploadService>();

// Per-circuit, so one browser tab's refresh notifications do not fan out to every other session.
builder.Services.AddScoped<UiEvents>();

var app = builder.Build();

if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error", createScopeForErrors: true);
    app.UseHsts();
}

app.UseStatusCodePagesWithReExecute("/not-found", createScopeForStatusCodePages: true);
app.UseHttpsRedirection();
app.UseAntiforgery();

app.MapStaticAssets();
app.MapExportEndpoints();
app.MapBlobEndpoints();
app.MapRazorComponents<App>()
    .AddInteractiveServerRenderMode();

app.Run();
