using Shiny.DocumentDb.Sqlite;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

public class SeedingTests : IDisposable
{
    readonly SqliteDocumentStore store;
    readonly string path;

    public SeedingTests()
    {
        this.path = Path.Combine(Path.GetTempPath(), $"seed_{Guid.NewGuid():N}.db");
        this.store = new SqliteDocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider($"Data Source={this.path}")
        });
    }

    public void Dispose()
    {
        this.store.Dispose();
        if (File.Exists(this.path))
            File.Delete(this.path);
    }

    sealed class UserSeeder(int version, params User[] users) : IDocumentSeeder
    {
        public string Name => "users";
        public int Version { get; } = version;
        public async Task SeedAsync(IDocumentStore store, CancellationToken cancellationToken)
        {
            foreach (var user in users)
                await store.Upsert(user, cancellationToken: cancellationToken);
        }
    }

    [Fact]
    public async Task RunsSeederAndRecordsMarker()
    {
        var applied = await DocumentSeedRunner.RunAsync(
            this.store,
            [new UserSeeder(1, new User { Id = "u1", Name = "Alice", Age = 25 })]);

        Assert.Equal(["users"], applied);
        Assert.Equal(1, await this.store.Count<User>());

        var marker = await this.store.Get<DocumentSeedMarker>("users");
        Assert.NotNull(marker);
        Assert.Equal(1, marker!.Version);
    }

    [Fact]
    public async Task DoesNotReRunSameVersion()
    {
        var seeder = new UserSeeder(1, new User { Id = "u1", Name = "Alice", Age = 25 });
        await DocumentSeedRunner.RunAsync(this.store, [seeder]);

        var secondRun = await DocumentSeedRunner.RunAsync(this.store, [seeder]);

        Assert.Empty(secondRun);
        Assert.Equal(1, await this.store.Count<User>());
    }

    [Fact]
    public async Task ReRunsWhenVersionBumped()
    {
        await DocumentSeedRunner.RunAsync(
            this.store,
            [new UserSeeder(1, new User { Id = "u1", Name = "Alice", Age = 25 })]);

        var applied = await DocumentSeedRunner.RunAsync(
            this.store,
            [new UserSeeder(2, new User { Id = "u2", Name = "Bob", Age = 30 })]);

        Assert.Equal(["users"], applied);
        Assert.Equal(2, await this.store.Count<User>());

        var marker = await this.store.Get<DocumentSeedMarker>("users");
        Assert.Equal(2, marker!.Version);
    }
}
