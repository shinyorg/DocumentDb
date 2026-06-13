using System.Net;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Runtime;
using Shiny.DocumentDb.PostgreSql;
using Xunit;

namespace Shiny.DocumentDb.Orleans.Tests;

[Collection("Postgres")]
public class MembershipTableTests(PostgresFixture fixture)
{
    static readonly IServiceProvider EmptyServices = new ServiceCollection().BuildServiceProvider();
    int port = 11_000;

    DocumentDbMembershipTable Create()
    {
        var options = Options.Create(new DocumentDbMembershipOptions
        {
            DatabaseProvider = new PostgreSqlDatabaseProvider(fixture.Container.GetConnectionString()),
            TableName = "membership_" + Guid.NewGuid().ToString("N")
        });
        var cluster = Options.Create(new ClusterOptions { ServiceId = "svc", ClusterId = "cluster" });
        return new DocumentDbMembershipTable(options, cluster, EmptyServices, NullLogger<DocumentDbMembershipTable>.Instance);
    }

    MembershipEntry Entry(SiloStatus status = SiloStatus.Active)
    {
        var silo = SiloAddress.New(new IPEndPoint(IPAddress.Loopback, Interlocked.Increment(ref this.port)), 0);
        return new MembershipEntry
        {
            SiloAddress = silo,
            Status = status,
            SiloName = "silo",
            HostName = "host",
            StartTime = DateTime.UtcNow,
            IAmAliveTime = DateTime.UtcNow,
            SuspectTimes = new()
        };
    }

    [Fact]
    public async Task Insert_Read_Update_Flow()
    {
        var table = this.Create();
        await table.InitializeMembershipTable(true);

        var empty = await table.ReadAll();
        Assert.Empty(empty.Members);

        var entry = this.Entry();
        var inserted = await table.InsertRow(entry, empty.Version.Next());
        Assert.True(inserted);

        var afterInsert = await table.ReadAll();
        Assert.Single(afterInsert.Members);
        var rowEtag = afterInsert.Members[0].Item2;

        // Duplicate insert of the same silo fails.
        Assert.False(await table.InsertRow(entry, afterInsert.Version.Next()));

        // Update with the current row etag succeeds.
        entry.Status = SiloStatus.ShuttingDown;
        Assert.True(await table.UpdateRow(entry, rowEtag, afterInsert.Version.Next()));

        // Update with the now-stale row etag fails (CAS).
        var afterUpdate = await table.ReadAll();
        Assert.False(await table.UpdateRow(entry, rowEtag, afterUpdate.Version.Next()));
    }

    [Fact]
    public async Task UpdateIAmAlive_Updates_Heartbeat()
    {
        var table = this.Create();
        await table.InitializeMembershipTable(true);
        var entry = this.Entry();
        await table.InsertRow(entry, (await table.ReadAll()).Version.Next());

        var newTime = DateTime.UtcNow.AddMinutes(5);
        entry.IAmAliveTime = newTime;
        await table.UpdateIAmAlive(entry);

        var row = await table.ReadRow(entry.SiloAddress);
        Assert.Single(row.Members);
        Assert.Equal(newTime, row.Members[0].Item1.IAmAliveTime, TimeSpan.FromSeconds(1));
    }

    [Fact]
    public async Task Delete_All_Entries()
    {
        var table = this.Create();
        await table.InitializeMembershipTable(true);
        await table.InsertRow(this.Entry(), (await table.ReadAll()).Version.Next());

        await table.DeleteMembershipTableEntries("cluster");

        var data = await table.ReadAll();
        Assert.Empty(data.Members);
    }
}
