using System.Net;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.GrainDirectory;
using Orleans.Runtime;
using Shiny.DocumentDb.PostgreSql;
using Xunit;

namespace Shiny.DocumentDb.Orleans.Tests;

[Collection("Postgres")]
public class GrainDirectoryTests(PostgresFixture fixture)
{
    static readonly IServiceProvider EmptyServices = new ServiceCollection().BuildServiceProvider();

    DocumentDbGrainDirectory Create()
    {
        var options = new DocumentDbGrainDirectoryOptions
        {
            DatabaseProvider = new PostgreSqlDatabaseProvider(fixture.Container.GetConnectionString()),
            TableName = "directory_" + Guid.NewGuid().ToString("N")
        };
        return new DocumentDbGrainDirectory("dir", options, EmptyServices, NullLogger<DocumentDbGrainDirectory>.Instance);
    }

    static GrainAddress Address(GrainId grain) => new()
    {
        GrainId = grain,
        ActivationId = ActivationId.NewId(),
        SiloAddress = SiloAddress.New(new IPEndPoint(IPAddress.Loopback, 22_222), 0),
        MembershipVersion = new MembershipVersion(1)
    };

    [Fact]
    public async Task Register_And_Lookup()
    {
        var dir = this.Create();
        var addr = Address(GrainId.Create("g", Guid.NewGuid().ToString("N")));

        var registered = await dir.Register(addr);
        Assert.Equal(addr.ActivationId, registered!.ActivationId);

        var looked = await dir.Lookup(addr.GrainId);
        Assert.NotNull(looked);
        Assert.Equal(addr.ActivationId, looked!.ActivationId);
    }

    [Fact]
    public async Task Register_FirstWriterWins()
    {
        var dir = this.Create();
        var grain = GrainId.Create("g", Guid.NewGuid().ToString("N"));
        var first = Address(grain);
        var second = Address(grain);

        await dir.Register(first);
        var result = await dir.Register(second); // no previousAddress -> existing wins

        Assert.Equal(first.ActivationId, result!.ActivationId);
    }

    [Fact]
    public async Task Register_WithPrevious_Replaces()
    {
        var dir = this.Create();
        var grain = GrainId.Create("g", Guid.NewGuid().ToString("N"));
        var first = Address(grain);
        var second = Address(grain);

        await dir.Register(first);
        var result = await dir.Register(second, first); // CAS on the previous activation

        Assert.Equal(second.ActivationId, result!.ActivationId);
        var looked = await dir.Lookup(grain);
        Assert.Equal(second.ActivationId, looked!.ActivationId);
    }

    [Fact]
    public async Task Unregister_Removes()
    {
        var dir = this.Create();
        var addr = Address(GrainId.Create("g", Guid.NewGuid().ToString("N")));
        await dir.Register(addr);

        await dir.Unregister(addr);

        Assert.Null(await dir.Lookup(addr.GrainId));
    }
}
