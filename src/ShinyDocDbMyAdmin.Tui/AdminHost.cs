using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using ShinyDocDbMyAdmin.Services;
using ShinyDocDbMyAdmin.Tui.Cli;

namespace ShinyDocDbMyAdmin.Tui;

/// <summary>
/// Builds the service container the terminal front end runs on.
/// </summary>
/// <remarks>
/// <para>
/// Deliberately the same registrations, in the same order, as the web front end's <c>Program.cs</c>.
/// The point of the shared Core project is that both apps get the same <see cref="ProfileStore"/> over
/// the same <c>~/.shinydocdbmyadmin/admin.db</c>, protected by the same <see cref="SecretProtector"/>
/// key file - so a connection saved in one is a connection the other opens, not a format they happen
/// to agree on.
/// </para>
/// <para>
/// What is missing next to the web app is everything that only exists because it is a web app:
/// no circuit-scoped <c>UiEvents</c>, no upload service, no demo seeding. Demo mode is a deployment
/// shape for a public URL; a tool you ran on your own machine is not one.
/// </para>
/// </remarks>
public static class AdminHost
{
    public static ServiceProvider Build(CommandLineOptions options) => Build(BuildConfiguration(options));

    /// <summary>
    /// The same container from configuration alone, so the render tests and the screenshot harness can
    /// point the tool at a scratch database without going through a command line.
    /// </summary>
    public static ServiceProvider Build(IConfiguration configuration)
    {
        // Only one SQLitePCLRaw bundle ships in the output (see ShinyDocDbMyAdmin.Core.csproj):
        // e_sqlcipher, which opens both plain and encrypted SQLite files. Initialising it here makes
        // that choice visible rather than leaving it to whichever bundle Microsoft.Data.Sqlite finds
        // first - and the profile store itself is a SQLite database, so it has to happen before the
        // container is built.
        SQLitePCL.Batteries_V2.Init();

        var services = new ServiceCollection();

        services.AddSingleton<IConfiguration>(configuration);
        services.AddLogging(logging =>
        {
            logging.AddConfiguration(configuration.GetSection("Logging"));

            // No console logger. The console is the UI - a stray log line would corrupt the frame
            // mid-render. Anything worth telling the user about surfaces as a toast or an error panel;
            // anything else goes to the file sink below.
            logging.AddProvider(new FileLoggerProvider(configuration));
        });

        services.AddSingleton<DemoMode>();
        services.AddSingleton<AppPaths>();
        services.AddSingleton<SecretProtector>();
        services.AddSingleton<ProvidedConnections>();
        services.AddSingleton<ProfileStore>();
        services.AddSingleton<ConnectionManager>();
        services.AddSingleton<DocumentAdminService>();
        services.AddSingleton<ImportExportService>();
        services.AddSingleton<ConnectionTransferService>();

        // Always registered - the UI needs something to ask - but when the assistant is switched off,
        // nothing behind it is, so an Assistant tab cannot appear even if a panel forgets to check.
        services.AddSingleton<AiAvailability>();
        if (!configuration.GetValue(AiAvailability.DisableKey, false))
        {
            services.AddSingleton<AiClientFactory>();
            services.AddSingleton<AiToolSurface>();
        }

        return services.BuildServiceProvider();
    }

    static IConfiguration BuildConfiguration(CommandLineOptions options)
    {
        var overrides = new Dictionary<string, string?>();

        if (options.DataDirectory is not null)
            overrides["ShinyDocDbMyAdmin:DataDirectory"] = options.DataDirectory;

        if (options.NoAi)
            overrides[AiAvailability.DisableKey] = "true";

        // The optional settings file sits in the data directory rather than next to the assembly: a
        // global tool's install directory is replaced wholesale on every update, and configuration a
        // person wrote should survive that.
        var dataDirectory = options.DataDirectory
                            ?? Environment.GetEnvironmentVariable("SHINYDOCDBMYADMIN_DATA")
                            ?? Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), ".shinydocdbmyadmin");

        return new ConfigurationBuilder()
            .AddJsonFile(Path.Combine(dataDirectory, "appsettings.json"), optional: true, reloadOnChange: false)
            .AddEnvironmentVariables()
            .AddInMemoryCollection(overrides)
            .Build();
    }
}
