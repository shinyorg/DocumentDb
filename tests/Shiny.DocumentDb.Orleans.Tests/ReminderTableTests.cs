using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Orleans;
using Orleans.Configuration;
using Orleans.Runtime;
using Shiny.DocumentDb.PostgreSql;
using Xunit;

namespace Shiny.DocumentDb.Orleans.Tests;

[Collection("Postgres")]
public class ReminderTableTests(PostgresFixture fixture)
{
    static readonly IServiceProvider EmptyServices = new ServiceCollection().BuildServiceProvider();

    DocumentDbReminderTable Create()
    {
        var options = Options.Create(new DocumentDbReminderOptions
        {
            DatabaseProvider = new PostgreSqlDatabaseProvider(fixture.Container.GetConnectionString()),
            TableName = "reminders_" + Guid.NewGuid().ToString("N")
        });
        var cluster = Options.Create(new ClusterOptions { ServiceId = "svc", ClusterId = "cluster" });
        return new DocumentDbReminderTable(options, cluster, EmptyServices, NullLogger<DocumentDbReminderTable>.Instance);
    }

    static ReminderEntry Entry(GrainId g, string name, TimeSpan period) => new()
    {
        GrainId = g,
        ReminderName = name,
        StartAt = DateTime.UtcNow,
        Period = period
    };

    [Fact]
    public async Task Upsert_Then_ReadRow()
    {
        var table = this.Create();
        var grain = GrainId.Create("r", Guid.NewGuid().ToString("N"));

        var etag = await table.UpsertRow(Entry(grain, "rem", TimeSpan.FromMinutes(5)));
        Assert.Equal("1", etag);

        var row = await table.ReadRow(grain, "rem");
        Assert.NotNull(row);
        Assert.Equal("rem", row!.ReminderName);
        Assert.Equal(TimeSpan.FromMinutes(5), row.Period);
        Assert.Equal("1", row.ETag);
    }

    [Fact]
    public async Task Upsert_Increments_Etag()
    {
        var table = this.Create();
        var grain = GrainId.Create("r", Guid.NewGuid().ToString("N"));

        await table.UpsertRow(Entry(grain, "rem", TimeSpan.FromMinutes(5)));
        var etag2 = await table.UpsertRow(Entry(grain, "rem", TimeSpan.FromMinutes(10)));
        Assert.Equal("2", etag2);

        var row = await table.ReadRow(grain, "rem");
        Assert.Equal(TimeSpan.FromMinutes(10), row!.Period);
    }

    [Fact]
    public async Task ReadRows_ByGrain_And_FullRange()
    {
        var table = this.Create();
        var grain = GrainId.Create("r", Guid.NewGuid().ToString("N"));
        await table.UpsertRow(Entry(grain, "a", TimeSpan.FromMinutes(1)));
        await table.UpsertRow(Entry(grain, "b", TimeSpan.FromMinutes(2)));

        var byGrain = await table.ReadRows(grain);
        Assert.Equal(2, byGrain.Reminders.Count);

        // begin == end wraps the ring -> returns every row.
        var fullRange = await table.ReadRows(0, 0);
        Assert.Equal(2, fullRange.Reminders.Count);
    }

    [Fact]
    public async Task RemoveRow_Respects_Etag()
    {
        var table = this.Create();
        var grain = GrainId.Create("r", Guid.NewGuid().ToString("N"));
        var etag = await table.UpsertRow(Entry(grain, "rem", TimeSpan.FromMinutes(5)));

        Assert.False(await table.RemoveRow(grain, "rem", "999")); // wrong etag
        Assert.True(await table.RemoveRow(grain, "rem", etag));   // correct etag
        Assert.Null(await table.ReadRow(grain, "rem"));
    }
}
