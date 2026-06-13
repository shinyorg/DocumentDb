using System.Text.Json;
using System.Text.Json.Serialization;

namespace Shiny.DocumentDb.Orleans;

/// <summary>
/// Source-generated <see cref="JsonSerializerContext"/> for the provider's own envelope/document types
/// (grain state, reminders, membership, grain directory). Wiring this into the underlying store's
/// <see cref="JsonSerializerOptions.TypeInfoResolverChain"/> lets the store serialize these internal
/// documents without reflection. CamelCase keeps the stored JSON paths (e.g. <c>$.state</c>) stable.
/// </summary>
[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(GrainStateRecord))]
[JsonSerializable(typeof(ReminderDocument))]
[JsonSerializable(typeof(MembershipDocument))]
[JsonSerializable(typeof(MembershipVersionDocument))]
[JsonSerializable(typeof(GrainDirectoryDocument))]
internal partial class OrleansSystemJsonContext : JsonSerializerContext;

static class OrleansSystemJson
{
    /// <summary>
    /// Builds the <see cref="JsonSerializerOptions"/> for the underlying document store: a copy of the
    /// caller's options (if any) with <see cref="OrleansSystemJsonContext"/> appended to the resolver
    /// chain, so the provider's internal envelope types always resolve to source-generated metadata.
    /// </summary>
    public static JsonSerializerOptions StoreOptions(JsonSerializerOptions? userOptions)
    {
        var json = userOptions is null ? new JsonSerializerOptions() : new JsonSerializerOptions(userOptions);
        json.TypeInfoResolverChain.Add(OrleansSystemJsonContext.Default);
        return json;
    }
}
