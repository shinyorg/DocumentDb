using ShinyDocDbMyAdmin.Models;
using ShinyDocDbMyAdmin.Providers;

namespace ShinyDocDbMyAdmin.Services;

/// <summary>
/// Connections handed to the tool by whatever is hosting it - in practice an Aspire AppHost that
/// referenced a DocumentDb store into this resource - rather than saved by someone using the UI.
/// </summary>
/// <remarks>
/// The contract is the one <c>Shiny.DocumentDb.Aspire.Hosting</c> already emits for client apps:
/// <c>ConnectionStrings:{name}</c> for the connection string and <c>Shiny:DocumentDb:{name}:Provider</c>
/// for the backend it points at. A connection string with no matching Provider key is ignored, so an
/// AppHost that also injects Redis or a blob container does not litter the explorer.
/// <para>
/// These are never written to the profile store. They live as long as the process, and the AppHost is
/// the only thing that can change them - which is why the UI will not let you edit or delete one.
/// </para>
/// </remarks>
public sealed class ProvidedConnections
{
    /// <summary>Configuration section holding the per-store settings that accompany a connection string.</summary>
    public const string ConfigurationSection = "Shiny:DocumentDb";

    /// <summary>Prefix that keeps provided profile ids from colliding with saved (GUID) ones.</summary>
    public const string IdPrefix = "provided-";

    readonly Dictionary<string, ProvidedConnection> byId = new(StringComparer.OrdinalIgnoreCase);

    public ProvidedConnections(IConfiguration configuration, ILogger<ProvidedConnections> logger)
    {
        // A blanket read-only switch for everything the host hands over - the setting you want when the
        // tool is pointed at production. A store can still opt itself in individually.
        var readOnlyByDefault = configuration.GetValue("ShinyDocDbMyAdmin:ReadOnly", false);

        foreach (var entry in configuration.GetSection("ConnectionStrings").GetChildren())
        {
            var connectionString = entry.Value;
            if (string.IsNullOrWhiteSpace(connectionString))
                continue;

            var settings = configuration.GetSection($"{ConfigurationSection}:{entry.Key}");
            var providerName = settings["Provider"];
            if (string.IsNullOrWhiteSpace(providerName))
                continue;

            if (!TryParseProvider(providerName, out var kind))
            {
                logger.LogWarning(
                    "Ignoring provided connection '{Name}': '{Provider}' is not a backend this tool can administer.",
                    entry.Key,
                    providerName);

                continue;
            }

            var profile = new ConnectionProfile
            {
                Id = IdPrefix + entry.Key,
                Name = entry.Key,
                Provider = kind,
                ReadOnly = settings.GetValue("ReadOnly", readOnlyByDefault)
            };

            this.byId[profile.Id] = new ProvidedConnection(profile, connectionString, settings["Password"]);
        }

        this.Profiles =
        [
            .. this.byId.Values
                .Select(x => x.Profile)
                .OrderBy(x => x.Name, StringComparer.OrdinalIgnoreCase)
        ];

        if (this.Profiles.Count > 0)
            logger.LogInformation("Picked up {Count} connection(s) from the host environment.", this.Profiles.Count);
    }

    /// <summary>The provided connections, as profiles the rest of the UI can treat like any other.</summary>
    public IReadOnlyList<ConnectionProfile> Profiles { get; }

    public bool IsProvided(string profileId) => this.byId.ContainsKey(profileId);

    public ProvidedConnection? Find(string profileId) => this.byId.GetValueOrDefault(profileId);

    /// <summary>
    /// Maps a provider name off the wire onto a <see cref="ProviderKind"/>. The hosting integration's
    /// <c>DocumentProviderKind</c> agrees with this tool on every name except Postgres, and SqlCipher /
    /// DuckDb / Oracle exist only here - so an unknown name is a warning, not a failure.
    /// </summary>
    static bool TryParseProvider(string value, out ProviderKind kind)
    {
        kind = default;

        var name = value.Trim();
        if (name.Length == 0 || char.IsDigit(name[0]))
            return false;

        if (name.Equals("Postgres", StringComparison.OrdinalIgnoreCase))
            name = nameof(ProviderKind.PostgreSql);

        return Enum.TryParse(name, ignoreCase: true, out kind) && Enum.IsDefined(kind);
    }
}

/// <summary>A provided connection and its plaintext secrets, which never reach the profile store.</summary>
public sealed record ProvidedConnection(ConnectionProfile Profile, string ConnectionString, string? Password);
