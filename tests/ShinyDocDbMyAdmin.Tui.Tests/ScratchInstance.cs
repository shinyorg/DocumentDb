using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using ShinyDocDbMyAdmin.Tui;
using ShinyDocDbMyAdmin.Tui.Cli;
using ShinyDocDbMyAdmin.Tui.Shell;

namespace ShinyDocDbMyAdmin.Tui.Tests;

/// <summary>
/// A throwaway instance of the tool: its own data directory, its own SQLite database, and a host-
/// provided connection pointing at it.
/// </summary>
/// <remarks>
/// Host-provided rather than a saved profile because it needs no secret protector round-trip and
/// cannot be edited away by a test - the connection is a fixture, not a thing under test.
/// </remarks>
public sealed class ScratchInstance : IDisposable
{
    readonly ServiceProvider services;

    public ScratchInstance(string? seedSql = null, bool readOnly = false)
    {
        this.DataDirectory = Path.Combine(Path.GetTempPath(), $"docdbtui_{Guid.NewGuid():N}");
        Directory.CreateDirectory(this.DataDirectory);

        this.DatabasePath = Path.Combine(this.DataDirectory, "scratch.db");

        SQLitePCL.Batteries_V2.Init();
        using (var connection = new SqliteConnection($"Data Source={this.DatabasePath}"))
        {
            connection.Open();
            using var command = connection.CreateCommand();
            command.CommandText = seedSql ?? SampleData.Sql;
            command.ExecuteNonQuery();
        }

        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["ShinyDocDbMyAdmin:DataDirectory"] = this.DataDirectory,
                ["ConnectionStrings:Scratch"] = $"Data Source={this.DatabasePath}",
                ["Shiny:DocumentDb:Scratch:Provider"] = "Sqlite",
                ["Shiny:DocumentDb:Scratch:ReadOnly"] = readOnly ? "true" : "false",

                // No assistant in tests: it would need a provider key, and nothing here exercises it.
                ["ShinyDocDbMyAdmin:DisableAi"] = "true"
            })
            .Build();

        this.services = AdminHost.Build(configuration);
        this.Shell = new AdminShell(this.services, new CommandLineOptions());
    }

    public string DataDirectory { get; }
    public string DatabasePath { get; }
    public AdminShell Shell { get; }
    public IServiceProvider Services => this.services;

    /// <summary>The profile id the host-provided connection gets.</summary>
    public const string ProfileId = "provided-Scratch";

    public void Dispose()
    {
        this.Shell.Shutdown();
        this.services.Dispose();

        // SQLite keeps the file mapped until the pool is emptied, which would leave the directory
        // undeletable on Windows.
        SqliteConnection.ClearAllPools();

        try
        {
            Directory.Delete(this.DataDirectory, recursive: true);
        }
        catch (IOException)
        {
            // A leftover temp directory is not worth failing a green test over.
        }
    }
}
