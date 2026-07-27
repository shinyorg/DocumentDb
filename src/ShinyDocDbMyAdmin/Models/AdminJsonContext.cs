using System.Text.Json.Serialization;

namespace ShinyDocDbMyAdmin.Models;

/// <summary>Source-generated serialization for the tool's own documents, so its store runs reflection-free.</summary>
[JsonSourceGenerationOptions(
    PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase,
    UseStringEnumConverter = true,
    WriteIndented = false)]
[JsonSerializable(typeof(ConnectionProfile))]
[JsonSerializable(typeof(SavedQuery))]
public partial class AdminJsonContext : JsonSerializerContext;

/// <summary>A statement kept in the query console's history for a connection.</summary>
public class SavedQuery
{
    public string Id { get; set; } = Guid.NewGuid().ToString("N");
    public string ProfileId { get; set; } = "";
    public string Name { get; set; } = "";
    public string Sql { get; set; } = "";
    public DateTimeOffset SavedAt { get; set; } = DateTimeOffset.UtcNow;
}
