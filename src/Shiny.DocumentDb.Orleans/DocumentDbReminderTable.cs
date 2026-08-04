using System.Globalization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using global::Orleans;
using global::Orleans.Configuration;
using global::Orleans.Runtime;

namespace Shiny.DocumentDb.Orleans;

public class DocumentDbReminderOptions : OrleansStoreOptions;

/// <summary>Persisted reminder row. Stored as a queryable document keyed by service + grain + name.</summary>
public sealed class ReminderDocument
{
    public string Id { get; set; } = null!;
    public int Version { get; set; }
    public string ServiceId { get; set; } = null!;
    /// <summary>The grain's uniform hash (a <see cref="uint"/> widened to <see cref="long"/> for range scans).</summary>
    public long GrainHash { get; set; }
    public string GrainId { get; set; } = null!;
    public string ReminderName { get; set; } = null!;
    public DateTime StartAt { get; set; }
    public long PeriodTicks { get; set; }
}

/// <summary>
/// An Orleans <see cref="IReminderTable"/> backed by <see cref="IDocumentStore"/>. Range reads use the
/// generic fluent query (hash predicate), and per-row CAS rides on the document version property.
/// </summary>
public sealed class DocumentDbReminderTable : IReminderTable
{
    const string DefaultTable = "orleans_reminders";

    readonly IDocumentStore store;
    readonly string serviceId;
    readonly ILogger<DocumentDbReminderTable> logger;

    public DocumentDbReminderTable(
        IOptions<DocumentDbReminderOptions> options,
        IOptions<ClusterOptions> clusterOptions,
        IServiceProvider services,
        ILogger<DocumentDbReminderTable> logger
    )
    {
        this.serviceId = clusterOptions.Value.ServiceId;
        this.logger = logger;
        this.store = OrleansDocumentStore.Build(options.Value, services, DefaultTable, (dso, table) =>
        {
            dso.ConfigureDocument<ReminderDocument>(cfg =>
            {
                cfg.Table = table;
                cfg.MapVersionProperty(x => x.Version);
            });
        });
    }

    public Task Init() => this.store.Count<ReminderDocument>();
    public Task StartAsync(CancellationToken cancellationToken) => this.Init();
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    public Task TestOnlyClearTable() => this.store.Clear<ReminderDocument>();

    string MakeId(GrainId grainId, string reminderName) => $"{this.serviceId}|{grainId}|{reminderName}";

    static ReminderEntry ToEntry(ReminderDocument d) => new()
    {
        GrainId = GrainId.Parse(d.GrainId),
        ReminderName = d.ReminderName,
        StartAt = d.StartAt,
        Period = TimeSpan.FromTicks(d.PeriodTicks),
        ETag = d.Version.ToString(CultureInfo.InvariantCulture)
    };

    public async Task<ReminderTableData> ReadRows(GrainId grainId)
    {
        var gid = grainId.ToString();
        var docs = await this.store.Query<ReminderDocument>()
            .Where(r => r.ServiceId == this.serviceId && r.GrainId == gid)
            .ToList()
            .ConfigureAwait(false);

        return new ReminderTableData(docs.Select(ToEntry).ToList());
    }

    public async Task<ReminderTableData> ReadRows(uint begin, uint end)
    {
        long b = begin, e = end;
        var query = this.store.Query<ReminderDocument>().Where(r => r.ServiceId == this.serviceId);

        // Exclusive of begin, inclusive of end. When begin >= end the range wraps the hash ring.
        query = begin < end
            ? query.Where(r => r.GrainHash > b && r.GrainHash <= e)
            : query.Where(r => r.GrainHash > b || r.GrainHash <= e);

        var docs = await query.ToList().ConfigureAwait(false);
        return new ReminderTableData(docs.Select(ToEntry).ToList());
    }

    public async Task<ReminderEntry?> ReadRow(GrainId grainId, string reminderName)
    {
        var doc = await this.store.Get<ReminderDocument>(this.MakeId(grainId, reminderName)).ConfigureAwait(false);
        return doc is null ? null : ToEntry(doc);
    }

    public async Task<string> UpsertRow(ReminderEntry entry)
    {
        var gid = entry.GrainId.ToString();
        var id = this.MakeId(entry.GrainId, entry.ReminderName);

        // Reminder upsert is last-writer-wins; retry on the rare concurrent write to the same row.
        for (var attempt = 0; attempt < 5; attempt++)
        {
            var existing = await this.store.Get<ReminderDocument>(id).ConfigureAwait(false);
            var doc = new ReminderDocument
            {
                Id = id,
                ServiceId = this.serviceId,
                GrainHash = entry.GrainId.GetUniformHashCode(),
                GrainId = gid,
                ReminderName = entry.ReminderName,
                StartAt = entry.StartAt,
                PeriodTicks = entry.Period.Ticks
            };
            try
            {
                if (existing is null)
                {
                    await this.store.Insert(doc).ConfigureAwait(false); // version -> 1
                }
                else
                {
                    doc.Version = existing.Version;
                    await this.store.Update(doc).ConfigureAwait(false); // CAS -> version + 1
                }
                return doc.Version.ToString(CultureInfo.InvariantCulture);
            }
            catch (ConcurrencyException) { /* another writer won; retry */ }
            catch (InvalidOperationException) when (existing is null) { /* lost the insert race; retry as update */ }
        }
        throw new InvalidOperationException($"Reminder '{entry.ReminderName}' upsert failed after retries due to contention.");
    }

    public async Task<bool> RemoveRow(GrainId grainId, string reminderName, string eTag)
    {
        if (!int.TryParse(eTag, NumberStyles.Integer, CultureInfo.InvariantCulture, out var version))
            return false;

        var id = this.MakeId(grainId, reminderName);
        var deleted = await this.store.Query<ReminderDocument>()
            .Where(r => r.Id == id && r.Version == version)
            .ExecuteDelete()
            .ConfigureAwait(false);

        return deleted > 0;
    }
}
