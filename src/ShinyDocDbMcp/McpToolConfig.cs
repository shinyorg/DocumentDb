using System.Text.Json;
using System.Text.Json.Serialization;
using ShinyDocDbMyAdmin.Providers;

namespace ShinyDocDbMcp;

/// <summary>
/// What the server was asked to expose. Assembled from the command line, optionally from a JSON config file,
/// and — for a saved connection — from the admin tool's profile store.
/// </summary>
sealed class McpToolConfig
{
    public string? Profile { get; set; }
    public ProviderKind? Provider { get; set; }
    public string? ConnectionString { get; set; }
    public string? Password { get; set; }
    public string Table { get; set; } = "documents";
    public bool AllowWrites { get; set; }
    public int MaxPageSize { get; set; } = 100;

    /// <summary>
    /// The collections to expose. Empty means "discover them" — every stored <c>TypeName</c> becomes an
    /// open-field collection.
    /// </summary>
    public List<McpCollectionConfig> Collections { get; set; } = new();

    public static McpToolConfig Parse(string[] args)
    {
        var config = new McpToolConfig();
        string? configPath = null;
        string? types = null;

        for (var i = 0; i < args.Length; i++)
        {
            var value = i + 1 < args.Length ? args[i + 1] : null;
            switch (args[i])
            {
                case "--profile":
                    config.Profile = Require(value, "--profile");
                    i++;
                    break;
                case "--provider":
                    config.Provider = ParseProvider(Require(value, "--provider"));
                    i++;
                    break;
                case "--connection":
                    config.ConnectionString = Require(value, "--connection");
                    i++;
                    break;
                case "--password":
                    config.Password = Require(value, "--password");
                    i++;
                    break;
                case "--table":
                    config.Table = Require(value, "--table");
                    i++;
                    break;
                case "--types":
                    types = Require(value, "--types");
                    i++;
                    break;
                case "--max-page-size":
                    config.MaxPageSize = int.Parse(Require(value, "--max-page-size"));
                    i++;
                    break;
                case "--config":
                    configPath = Require(value, "--config");
                    i++;
                    break;
                case "--allow-writes":
                    config.AllowWrites = true;
                    break;
                case "--help" or "-h":
                    throw new UsageException(null);
                default:
                    throw new UsageException($"Unknown argument '{args[i]}'.");
            }
        }

        if (configPath != null)
        {
            var file = JsonSerializer.Deserialize(
                File.ReadAllText(configPath),
                McpToolJsonContext.Default.McpToolConfig)
                ?? throw new UsageException($"'{configPath}' is empty or not a JSON object.");

            // Command-line arguments win over the file — the file is the shape of the exposure, the command
            // line is which database you pointed it at today.
            file.Profile = config.Profile ?? file.Profile;
            file.Provider = config.Provider ?? file.Provider;
            file.ConnectionString = config.ConnectionString ?? file.ConnectionString;
            file.Password = config.Password ?? file.Password;
            file.AllowWrites |= config.AllowWrites;
            config = file;
        }

        if (types != null)
        {
            config.Collections = types
                .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .Select(name => new McpCollectionConfig { Name = name })
                .ToList();
        }

        if (config.Profile is null && (config.Provider is null || string.IsNullOrWhiteSpace(config.ConnectionString)))
            throw new UsageException("Supply either --profile <name>, or --provider <kind> --connection <string>.");

        if (config.MaxPageSize <= 0)
            throw new UsageException("--max-page-size must be positive.");

        return config;
    }

    static string Require(string? value, string name)
        => value is null || value.StartsWith("--", StringComparison.Ordinal)
            ? throw new UsageException($"{name} needs a value.")
            : value;

    static ProviderKind ParseProvider(string name)
        => Enum.TryParse<ProviderKind>(name, ignoreCase: true, out var kind)
            ? kind
            : throw new UsageException(
                $"Unknown provider '{name}'. Known: {string.Join(", ", Enum.GetNames<ProviderKind>())}.");
}

sealed class McpCollectionConfig
{
    /// <summary>The stored <c>TypeName</c> discriminator, which is also the tool slug.</summary>
    public string Name { get; set; } = "";

    public string IdProperty { get; set; } = "id";
    public string? Description { get; set; }

    /// <summary>A non-removable string-grammar clause, e.g. <c>tenantId == 'acme'</c>. Read-only by nature.</summary>
    public string? Where { get; set; }

    /// <summary>Declared fields. Empty means open-field mode (the model may name any path).</summary>
    public List<McpFieldConfig> Fields { get; set; } = new();
}

sealed class McpFieldConfig
{
    public string Path { get; set; } = "";
    public string Type { get; set; } = "string";
    public string? Description { get; set; }
}

sealed class UsageException(string? message) : Exception(message ?? "");

[JsonSourceGenerationOptions(PropertyNameCaseInsensitive = true, PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(McpToolConfig))]
partial class McpToolJsonContext : JsonSerializerContext;
