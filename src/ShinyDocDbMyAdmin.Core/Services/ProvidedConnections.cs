using System.Text;
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

    /// <summary>
    /// Per-store setting carrying a base64 connection string, for hosts that cannot deliver a literal
    /// one. Every relational connection string contains <c>;</c>, and Docker Desktop's extension SDK -
    /// which builds a shell command line out of the arguments it is handed - rejects an argument that
    /// <c>shell-quote</c> reads as containing an operator. Base64's alphabet has no operator, no glob
    /// and no space, so it survives; this is what the Docker Desktop extension sends.
    /// </summary>
    public const string ConnectionStringBase64Setting = "ConnectionStringBase64";

    /// <summary>Prefix that keeps provided profile ids from colliding with saved (GUID) ones.</summary>
    public const string IdPrefix = "provided-";

    /// <summary>
    /// Per-store section declaring which document types the application configured for soft delete, keyed by
    /// stored type name. Either shape works:
    /// <code>
    /// Shiny:DocumentDb:store:SoftDelete:Customer            = isDeleted
    /// Shiny:DocumentDb:store:SoftDelete:Invoice:PropertyPath = deletedAt
    /// Shiny:DocumentDb:store:SoftDelete:Invoice:FlagKind     = Timestamp
    /// </code>
    /// </summary>
    /// <remarks>
    /// It has to be here rather than only in the UI: a provided connection is never written to the profile
    /// store, so there would otherwise be nowhere to declare one - and an Aspire-hosted admin would keep
    /// hard-deleting documents its application only ever flags. The host declares the connection, so the
    /// host declares its soft-deleted types.
    /// </remarks>
    public const string SoftDeleteSetting = "SoftDelete";

    readonly Dictionary<string, ProvidedConnection> byId = new(StringComparer.OrdinalIgnoreCase);

    public ProvidedConnections(IConfiguration configuration, ILogger<ProvidedConnections> logger)
    {
        // A blanket read-only switch for everything the host hands over - the setting you want when the
        // tool is pointed at production. A store can still opt itself in individually.
        var readOnlyByDefault = configuration.GetValue("ShinyDocDbMyAdmin:ReadOnly", false);

        // A store can arrive as a ConnectionStrings entry, or as a Shiny:DocumentDb entry carrying a
        // base64 connection string and nothing else. Walking the union of both is what keeps the
        // second shape from being invisible.
        var names = configuration
            .GetSection("ConnectionStrings").GetChildren()
            .Concat(configuration.GetSection(ConfigurationSection).GetChildren())
            .Select(x => x.Key)
            .Distinct(StringComparer.OrdinalIgnoreCase);

        foreach (var name in names)
        {
            var settings = configuration.GetSection($"{ConfigurationSection}:{name}");
            var connectionString = ResolveConnectionString(
                configuration[$"ConnectionStrings:{name}"],
                settings[ConnectionStringBase64Setting],
                name,
                logger);

            var providerName = settings["Provider"];

            if (!string.IsNullOrWhiteSpace(connectionString) && !string.IsNullOrWhiteSpace(providerName))
            {
                if (!TryParseProvider(providerName, out var kind))
                {
                    logger.LogWarning(
                        "Ignoring provided connection '{Name}': '{Provider}' is not a backend this tool can administer.",
                        name,
                        providerName);
                }
                else
                {
                    var profile = new ConnectionProfile
                    {
                        Id = IdPrefix + name,
                        Name = name,
                        Provider = kind,
                        ReadOnly = settings.GetValue("ReadOnly", readOnlyByDefault),
                        SoftDeleteFlags = [.. ReadSoftDeleteFlags(settings.GetSection(SoftDeleteSetting), name, logger)]
                    };

                    this.byId[profile.Id] = new ProvidedConnection(profile, connectionString, settings["Password"]);
                }
            }
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

    /// <summary>Reads the declared soft-delete flags for one store. See <see cref="SoftDeleteSetting"/>.</summary>
    static IEnumerable<SoftDeleteFlag> ReadSoftDeleteFlags(IConfigurationSection section, string name, ILogger logger)
    {
        foreach (var entry in section.GetChildren())
        {
            // The compact form is the value itself; the long form spells the two fields out. A boolean flag
            // is the common case, so it is what the compact form means.
            var path = entry.Value ?? entry["PropertyPath"];
            if (string.IsNullOrWhiteSpace(path))
            {
                logger.LogWarning(
                    "Ignoring soft-delete declaration '{Type}' on provided connection '{Name}': it names no property path.",
                    entry.Key, name);
                continue;
            }

            var kindName = entry.Value is null ? entry["FlagKind"] : null;
            if (!string.IsNullOrWhiteSpace(kindName) && !Enum.TryParse<SoftDeleteFlagKind>(kindName, ignoreCase: true, out _))
            {
                logger.LogWarning(
                    "Ignoring soft-delete declaration '{Type}' on provided connection '{Name}': '{Kind}' is not Boolean or Timestamp.",
                    entry.Key, name, kindName);
                continue;
            }

            yield return new SoftDeleteFlag
            {
                TypeName = entry.Key,
                PropertyPath = path.Trim(),
                FlagKind = string.IsNullOrWhiteSpace(kindName)
                    ? SoftDeleteFlagKind.Boolean
                    : Enum.Parse<SoftDeleteFlagKind>(kindName, ignoreCase: true)
            };
        }
    }

    /// <summary>
    /// The literal connection string if there is one, otherwise the base64 form decoded. A literal
    /// wins: it is the ordinary contract, and the encoded form exists only for hosts that cannot
    /// deliver one.
    /// </summary>
    static string? ResolveConnectionString(string? literal, string? base64, string name, ILogger logger)
    {
        if (!string.IsNullOrWhiteSpace(literal) || string.IsNullOrWhiteSpace(base64))
            return literal;

        try
        {
            return Encoding.UTF8.GetString(Convert.FromBase64String(base64.Trim()));
        }
        catch (FormatException)
        {
            logger.LogWarning(
                "Ignoring provided connection '{Name}': its {Setting} is not valid base64.",
                name,
                ConnectionStringBase64Setting);

            return null;
        }
    }

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
