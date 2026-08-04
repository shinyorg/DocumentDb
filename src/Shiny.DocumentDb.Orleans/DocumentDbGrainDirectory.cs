using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using global::Orleans.GrainDirectory;
using global::Orleans.Runtime;

namespace Shiny.DocumentDb.Orleans;

public class DocumentDbGrainDirectoryOptions : OrleansStoreOptions;

sealed class GrainDirectoryDocument
{
    public string Id { get; set; } = null!;
    public int Version { get; set; }
    public string GrainId { get; set; } = null!;
    public string ActivationId { get; set; } = null!;
    public string SiloAddress { get; set; } = null!;
    public long MembershipVersion { get; set; }
}

/// <summary>
/// An Orleans <see cref="IGrainDirectory"/> backed by <see cref="IDocumentStore"/>. Registration is a
/// register-once CAS (first writer wins); unregister and silo-cleanup are conditional deletes.
/// </summary>
public sealed class DocumentDbGrainDirectory : IGrainDirectory
{
    const string DefaultTable = "orleans_graindirectory";

    readonly IDocumentStore store;
    readonly string name;
    readonly ILogger<DocumentDbGrainDirectory> logger;

    public DocumentDbGrainDirectory(
        string name,
        DocumentDbGrainDirectoryOptions options,
        IServiceProvider services,
        ILogger<DocumentDbGrainDirectory> logger
    )
    {
        this.name = name;
        this.logger = logger;
        this.store = OrleansDocumentStore.Build(options, services, DefaultTable, (dso, table) =>
        {
            dso.ConfigureDocument<GrainDirectoryDocument>(cfg =>
            {
                cfg.Table = table;
                cfg.MapVersionProperty(x => x.Version);
            });
        });
    }

    internal static DocumentDbGrainDirectory Create(IServiceProvider services, string name)
    {
        var options = services.GetRequiredService<IOptionsMonitor<DocumentDbGrainDirectoryOptions>>().Get(name);
        return ActivatorUtilities.CreateInstance<DocumentDbGrainDirectory>(services, name, options);
    }

    static GrainAddress ToAddress(GrainDirectoryDocument d) => new()
    {
        GrainId = GrainId.Parse(d.GrainId),
        ActivationId = ActivationId.FromParsableString(d.ActivationId),
        SiloAddress = SiloAddress.FromParsableString(d.SiloAddress),
        MembershipVersion = new MembershipVersion(d.MembershipVersion)
    };

    static GrainDirectoryDocument ToDoc(GrainAddress a) => new()
    {
        Id = a.GrainId.ToString(),
        GrainId = a.GrainId.ToString(),
        ActivationId = a.ActivationId.ToParsableString(),
        SiloAddress = a.SiloAddress!.ToParsableString(),
        MembershipVersion = a.MembershipVersion.Value
    };

    public Task<GrainAddress?> Register(GrainAddress address) => this.Register(address, null);

    public async Task<GrainAddress?> Register(GrainAddress address, GrainAddress? previousAddress)
    {
        var id = address.GrainId.ToString();
        var existing = await this.store.Get<GrainDirectoryDocument>(id).ConfigureAwait(false);

        if (existing is null)
        {
            try
            {
                await this.store.Insert(ToDoc(address)).ConfigureAwait(false);
                return address;
            }
            catch (InvalidOperationException)
            {
                // Lost the insert race — return whoever won.
                var won = await this.store.Get<GrainDirectoryDocument>(id).ConfigureAwait(false);
                return won is null ? address : ToAddress(won);
            }
        }

        // Update path: only replace when the caller's previousAddress still matches the stored activation.
        if (previousAddress is null || existing.ActivationId != previousAddress.ActivationId.ToParsableString())
            return ToAddress(existing);

        var doc = ToDoc(address);
        doc.Version = existing.Version;
        try
        {
            await this.store.Update(doc).ConfigureAwait(false);
            return address;
        }
        catch (ConcurrencyException)
        {
            var current = await this.store.Get<GrainDirectoryDocument>(id).ConfigureAwait(false);
            return current is null ? address : ToAddress(current);
        }
    }

    public Task Unregister(GrainAddress address)
    {
        var id = address.GrainId.ToString();
        var activation = address.ActivationId.ToParsableString();
        return this.store.Query<GrainDirectoryDocument>()
            .Where(d => d.Id == id && d.ActivationId == activation)
            .ExecuteDelete();
    }

    public async Task<GrainAddress?> Lookup(GrainId grainId)
    {
        var doc = await this.store.Get<GrainDirectoryDocument>(grainId.ToString()).ConfigureAwait(false);
        return doc is null ? null : ToAddress(doc);
    }

    public async Task UnregisterSilos(List<SiloAddress> siloAddresses)
    {
        foreach (var silo in siloAddresses)
        {
            var parsable = silo.ToParsableString();
            await this.store.Query<GrainDirectoryDocument>()
                .Where(d => d.SiloAddress == parsable)
                .ExecuteDelete()
                .ConfigureAwait(false);
        }
    }
}
