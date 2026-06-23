using System.Text.Json;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Shiny.DocumentDb.AppDataSync;


/// <summary>
/// Startup check enforcing Hard edge #5: every sync-registered type must serialize identically through
/// DocumentDb's <see cref="JsonSerializerOptions"/> and Shiny.Data.Sync's <c>Json.Default</c>. A probe
/// instance is created and serialized through both; a divergence throws
/// <see cref="SyncSerializerMismatchException"/> at host start rather than letting outbound JSON fail to
/// round-trip silently.
/// </summary>
sealed class SyncSerializerValidator(
    SyncDocumentRegistry registry,
    DocumentStoreOptions storeOptions,
    ILogger<SyncSerializerValidator> logger
) : IHostedService
{
    static readonly JsonSerializerOptions DefaultStoreOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false
    };

    public Task StartAsync(CancellationToken cancellationToken)
    {
        var options = storeOptions.JsonSerializerOptions ?? DefaultStoreOptions;

        foreach (var registration in registry.All)
        {
            var probe = registration.CreateProbe();
            if (probe == null)
            {
                logger.LogDebug(
                    "Skipped serializer-equivalence check for '{Type}' — no probe instance could be created.",
                    registration.DocumentType.FullName
                );
                continue;
            }

            string storeJson;
            string syncJson;
            try
            {
                storeJson = registration.SerializeViaStore(probe, options);
                syncJson = registration.SerializeViaSync(probe);
            }
            catch (Exception ex)
            {
                logger.LogWarning(
                    ex,
                    "Could not validate serializer equivalence for '{Type}'.",
                    registration.DocumentType.FullName
                );
                continue;
            }

            if (!JsonEquivalent(storeJson, syncJson))
                throw new SyncSerializerMismatchException(registration.DocumentType, storeJson, syncJson);
        }
        return Task.CompletedTask;
    }


    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;


    // Compare structurally so insignificant key ordering does not trigger a false mismatch; a naming
    // policy / converter divergence (the thing we guard) still differs.
    static bool JsonEquivalent(string a, string b)
    {
        using var da = JsonDocument.Parse(a);
        using var db = JsonDocument.Parse(b);
        return ElementEquivalent(da.RootElement, db.RootElement);
    }


    static bool ElementEquivalent(JsonElement a, JsonElement b)
    {
        if (a.ValueKind != b.ValueKind)
            return false;

        switch (a.ValueKind)
        {
            case JsonValueKind.Object:
                var aProps = a.EnumerateObject().OrderBy(p => p.Name, StringComparer.Ordinal).ToList();
                var bProps = b.EnumerateObject().OrderBy(p => p.Name, StringComparer.Ordinal).ToList();
                if (aProps.Count != bProps.Count)
                    return false;
                for (var i = 0; i < aProps.Count; i++)
                {
                    if (!string.Equals(aProps[i].Name, bProps[i].Name, StringComparison.Ordinal))
                        return false;
                    if (!ElementEquivalent(aProps[i].Value, bProps[i].Value))
                        return false;
                }
                return true;

            case JsonValueKind.Array:
                var aItems = a.EnumerateArray().ToList();
                var bItems = b.EnumerateArray().ToList();
                if (aItems.Count != bItems.Count)
                    return false;
                for (var i = 0; i < aItems.Count; i++)
                {
                    if (!ElementEquivalent(aItems[i], bItems[i]))
                        return false;
                }
                return true;

            default:
                return a.GetRawText() == b.GetRawText();
        }
    }
}
