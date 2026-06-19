using System.Globalization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using global::Orleans;
using global::Orleans.Configuration;
using global::Orleans.Runtime;

namespace Shiny.DocumentDb.Orleans;

public class DocumentDbMembershipOptions : OrleansStoreOptions;

sealed class MembershipVersionDocument
{
    public string Id { get; set; } = null!;
    public int Version { get; set; }       // document CAS -> TableVersion.VersionEtag
    public string ClusterId { get; set; } = null!;
    public int TableVersion { get; set; }  // Orleans TableVersion.Version
}

sealed class MembershipDocument
{
    public string Id { get; set; } = null!;
    public int Version { get; set; }       // document CAS -> row ETag
    public string ClusterId { get; set; } = null!;
    public string SiloAddress { get; set; } = null!;
    public int Status { get; set; }
    /// <summary>UTC ticks — a scalar (not a JSON date) so the heartbeat <c>SetProperty</c> and the
    /// cleanup range query work uniformly across providers.</summary>
    public long IAmAliveTimeTicks { get; set; }
    public MembershipEntryDto Entry { get; set; } = null!;
}

sealed class MembershipEntryDto
{
    public string SiloAddress { get; set; } = null!;
    public int Status { get; set; }
    public string? SiloName { get; set; }
    public string? HostName { get; set; }
    public string? RoleName { get; set; }
    public int ProxyPort { get; set; }
    public int UpdateZone { get; set; }
    public int FaultZone { get; set; }
    public DateTime StartTime { get; set; }
    public DateTime IAmAliveTime { get; set; }
    public List<SuspectDto> SuspectTimes { get; set; } = new();
}

sealed class SuspectDto
{
    public string SiloAddress { get; set; } = null!;
    public DateTime Time { get; set; }
}

/// <summary>
/// An Orleans <see cref="IMembershipTable"/> backed by <see cref="IDocumentStore"/>. Membership and the
/// global table-version row are updated together in a single <see cref="UnitOfWork"/>,
/// each gated on its own version (CAS). This requires a backend with real multi-document transactions —
/// the relational providers, or MongoDB on a replica set. Cosmos (single-partition batches only) is not
/// supported.
/// </summary>
public sealed class DocumentDbMembershipTable : IMembershipTable
{
    const string DefaultTable = "orleans_membership";

    readonly IDocumentStore store;
    readonly string clusterId;
    readonly ILogger<DocumentDbMembershipTable> logger;

    public DocumentDbMembershipTable(
        IOptions<DocumentDbMembershipOptions> options,
        IOptions<ClusterOptions> clusterOptions,
        IServiceProvider services,
        ILogger<DocumentDbMembershipTable> logger
    )
    {
        this.clusterId = clusterOptions.Value.ClusterId;
        this.logger = logger;
        this.store = OrleansDocumentStore.Build(options.Value, services, DefaultTable, (dso, _) =>
        {
            // Both document types share the table, discriminated by TypeName.
            dso.MapVersionProperty<MembershipDocument>(x => x.Version);
            dso.MapVersionProperty<MembershipVersionDocument>(x => x.Version);
        });
    }

    string VersionId => $"{this.clusterId}|__version";
    string MemberId(SiloAddress silo) => $"{this.clusterId}|{silo.ToParsableString()}";

    static TableVersion ToTableVersion(MembershipVersionDocument? v) =>
        new(v?.TableVersion ?? 0, (v?.Version ?? 0).ToString(CultureInfo.InvariantCulture));

    public async Task InitializeMembershipTable(bool tryInitTableVersion)
    {
        await this.store.Count<MembershipVersionDocument>().ConfigureAwait(false);
        if (!tryInitTableVersion)
            return;

        var existing = await this.store.Get<MembershipVersionDocument>(this.VersionId).ConfigureAwait(false);
        if (existing is not null)
            return;

        try
        {
            await this.store.Insert(new MembershipVersionDocument
            {
                Id = this.VersionId,
                ClusterId = this.clusterId,
                TableVersion = 0
            }).ConfigureAwait(false);
        }
        catch (InvalidOperationException)
        {
            // Another silo created it first — fine.
        }
    }

    public async Task<MembershipTableData> ReadRow(SiloAddress key)
    {
        var doc = await this.store.Get<MembershipDocument>(this.MemberId(key)).ConfigureAwait(false);
        var versionDoc = await this.store.Get<MembershipVersionDocument>(this.VersionId).ConfigureAwait(false);

        var list = new List<Tuple<MembershipEntry, string>>();
        if (doc is not null)
            list.Add(Tuple.Create(ToEntry(doc), doc.Version.ToString(CultureInfo.InvariantCulture)));

        return new MembershipTableData(list, ToTableVersion(versionDoc));
    }

    public async Task<MembershipTableData> ReadAll()
    {
        var versionDoc = await this.store.Get<MembershipVersionDocument>(this.VersionId).ConfigureAwait(false);
        var members = await this.store.Query<MembershipDocument>()
            .Where(m => m.ClusterId == this.clusterId)
            .ToList()
            .ConfigureAwait(false);

        var list = members
            .Select(m => Tuple.Create(ToEntry(m), m.Version.ToString(CultureInfo.InvariantCulture)))
            .ToList();

        return new MembershipTableData(list, ToTableVersion(versionDoc));
    }

    public async Task<bool> InsertRow(MembershipEntry entry, TableVersion tableVersion)
    {
        try
        {
            var uow = this.store.CreateUnitOfWork();
            this.BumpVersionRow(uow, tableVersion);
            uow.Add(this.ToDocument(entry)); // SaveChanges throws if the silo row already exists
            await uow.SaveChanges().ConfigureAwait(false);
            return true;
        }
        catch (ConcurrencyException) { return false; }
        catch (InvalidOperationException) { return false; }
    }

    public async Task<bool> UpdateRow(MembershipEntry entry, string etag, TableVersion tableVersion)
    {
        if (!int.TryParse(etag, NumberStyles.Integer, CultureInfo.InvariantCulture, out var rowVersion))
            return false;

        try
        {
            var uow = this.store.CreateUnitOfWork();
            this.BumpVersionRow(uow, tableVersion);
            var doc = this.ToDocument(entry);
            doc.Version = rowVersion;
            uow.Update(doc); // CAS on the row ETag, enforced on SaveChanges
            await uow.SaveChanges().ConfigureAwait(false);
            return true;
        }
        catch (ConcurrencyException) { return false; }
        catch (InvalidOperationException) { return false; }
    }

    void BumpVersionRow(UnitOfWork uow, TableVersion tableVersion)
    {
        var expected = int.Parse(tableVersion.VersionEtag, CultureInfo.InvariantCulture);
        uow.Update(new MembershipVersionDocument
        {
            Id = this.VersionId,
            ClusterId = this.clusterId,
            Version = expected,            // CAS against the version row's current ETag
            TableVersion = tableVersion.Version
        });
    }

    public Task UpdateIAmAlive(MembershipEntry entry) =>
        // Heartbeat only — no version bump, no CAS. IAmAliveTimeTicks is a top-level field for exactly this.
        this.store.SetProperty<MembershipDocument>(this.MemberId(entry.SiloAddress), x => x.IAmAliveTimeTicks, entry.IAmAliveTime.Ticks);

    public async Task DeleteMembershipTableEntries(string clusterId)
    {
        await this.store.Query<MembershipDocument>().Where(m => m.ClusterId == clusterId).ExecuteDelete().ConfigureAwait(false);
        await this.store.Query<MembershipVersionDocument>().Where(v => v.ClusterId == clusterId).ExecuteDelete().ConfigureAwait(false);
    }

    public async Task CleanupDefunctSiloEntries(DateTimeOffset beforeDate)
    {
        var dead = (int)SiloStatus.Dead;
        var beforeTicks = beforeDate.UtcDateTime.Ticks;
        await this.store.Query<MembershipDocument>()
            .Where(m => m.ClusterId == this.clusterId && m.Status == dead && m.IAmAliveTimeTicks < beforeTicks)
            .ExecuteDelete()
            .ConfigureAwait(false);
    }

    MembershipDocument ToDocument(MembershipEntry e) => new()
    {
        Id = this.MemberId(e.SiloAddress),
        ClusterId = this.clusterId,
        SiloAddress = e.SiloAddress.ToParsableString(),
        Status = (int)e.Status,
        IAmAliveTimeTicks = e.IAmAliveTime.Ticks,
        Entry = new MembershipEntryDto
        {
            SiloAddress = e.SiloAddress.ToParsableString(),
            Status = (int)e.Status,
            SiloName = e.SiloName,
            HostName = e.HostName,
            RoleName = e.RoleName,
            ProxyPort = e.ProxyPort,
            UpdateZone = e.UpdateZone,
            FaultZone = e.FaultZone,
            StartTime = e.StartTime,
            IAmAliveTime = e.IAmAliveTime,
            SuspectTimes = (e.SuspectTimes ?? new())
                .Select(s => new SuspectDto { SiloAddress = s.Item1.ToParsableString(), Time = s.Item2 })
                .ToList()
        }
    };

    static MembershipEntry ToEntry(MembershipDocument d)
    {
        var dto = d.Entry;
        return new MembershipEntry
        {
            SiloAddress = SiloAddress.FromParsableString(dto.SiloAddress),
            Status = (SiloStatus)dto.Status,
            SiloName = dto.SiloName,
            HostName = dto.HostName,
            RoleName = dto.RoleName,
            ProxyPort = dto.ProxyPort,
            UpdateZone = dto.UpdateZone,
            FaultZone = dto.FaultZone,
            StartTime = dto.StartTime,
            IAmAliveTime = new DateTime(d.IAmAliveTimeTicks, DateTimeKind.Utc), // authoritative heartbeat value
            SuspectTimes = dto.SuspectTimes
                .Select(s => new Tuple<SiloAddress, DateTime>(SiloAddress.FromParsableString(s.SiloAddress), s.Time))
                .ToList()
        };
    }
}
