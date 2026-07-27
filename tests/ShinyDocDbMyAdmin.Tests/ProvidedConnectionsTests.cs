using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using ShinyDocDbMyAdmin.Providers;
using ShinyDocDbMyAdmin.Services;

namespace ShinyDocDbMyAdmin.Tests;

/// <summary>
/// The host-facing half of the Aspire integration: what the tool does with the environment an AppHost
/// hands it. Everything here is the contract <c>Shiny.DocumentDb.Aspire.Hosting</c> writes.
/// </summary>
public class ProvidedConnectionsTests
{
    static ProvidedConnections Build(params (string Key, string Value)[] settings)
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(settings.Select(x => new KeyValuePair<string, string?>(x.Key, x.Value)))
            .Build();

        return new ProvidedConnections(configuration, NullLogger<ProvidedConnections>.Instance);
    }

    [Fact]
    public void ConnectionStringWithAProvider_BecomesAConnection()
    {
        var provided = Build(
            ("ConnectionStrings:orders", "Data Source=/tmp/orders.db"),
            ("Shiny:DocumentDb:orders:Provider", "Sqlite"));

        var profile = Assert.Single(provided.Profiles);
        Assert.Equal("orders", profile.Name);
        Assert.Equal(ProviderKind.Sqlite, profile.Provider);
        Assert.False(profile.ReadOnly);
        Assert.True(provided.IsProvided(profile.Id));
        Assert.Equal("Data Source=/tmp/orders.db", provided.Find(profile.Id)!.ConnectionString);
    }

    [Fact]
    public void ConnectionStringWithoutAProvider_IsIgnored()
    {
        // An AppHost injects every reference it has - Redis, blob storage, a downstream API. Without
        // the provider discriminator we have no business trying to open it as a document store.
        var provided = Build(("ConnectionStrings:cache", "localhost:6379"));

        Assert.Empty(provided.Profiles);
    }

    [Fact]
    public void EmptyConnectionString_IsIgnored()
    {
        var provided = Build(
            ("ConnectionStrings:orders", ""),
            ("Shiny:DocumentDb:orders:Provider", "Sqlite"));

        Assert.Empty(provided.Profiles);
    }

    [Fact]
    public void UnknownProvider_IsIgnoredRatherThanFatal()
    {
        // MongoDB is a real DocumentDb backend, just not one this tool can administer - a store on it
        // should not take the whole UI down.
        var provided = Build(
            ("ConnectionStrings:events", "mongodb://localhost"),
            ("Shiny:DocumentDb:events:Provider", "MongoDb"));

        Assert.Empty(provided.Profiles);
    }

    [Fact]
    public void NumericProviderValue_IsRejected()
    {
        // Enum.TryParse happily accepts "3" and would silently pick whichever provider that is.
        var provided = Build(
            ("ConnectionStrings:orders", "Data Source=/tmp/orders.db"),
            ("Shiny:DocumentDb:orders:Provider", "3"));

        Assert.Empty(provided.Profiles);
    }

    [Theory]
    [InlineData("Postgres", ProviderKind.PostgreSql)]      // the hosting integration's spelling
    [InlineData("postgresql", ProviderKind.PostgreSql)]
    [InlineData("SqlServer", ProviderKind.SqlServer)]
    [InlineData("mysql", ProviderKind.MySql)]
    [InlineData("MariaDb", ProviderKind.MariaDb)]
    [InlineData("CockroachDb", ProviderKind.CockroachDb)]
    [InlineData("SqlCipher", ProviderKind.SqlCipher)]      // tool-only, no DocumentProviderKind for it
    [InlineData("DuckDb", ProviderKind.DuckDb)]
    [InlineData("Oracle", ProviderKind.Oracle)]
    public void ProviderNames_MapAcrossBothEnums(string name, ProviderKind expected)
    {
        var provided = Build(
            ("ConnectionStrings:store", "irrelevant"),
            ("Shiny:DocumentDb:store:Provider", name));

        Assert.Equal(expected, Assert.Single(provided.Profiles).Provider);
    }

    [Fact]
    public void ReadOnly_AppliesToEveryStore()
    {
        var provided = Build(
            ("ShinyDocDbMyAdmin:ReadOnly", "true"),
            ("ConnectionStrings:orders", "Data Source=/tmp/orders.db"),
            ("Shiny:DocumentDb:orders:Provider", "Sqlite"),
            ("ConnectionStrings:audit", "Data Source=/tmp/audit.db"),
            ("Shiny:DocumentDb:audit:Provider", "Sqlite"));

        Assert.Equal(2, provided.Profiles.Count);
        Assert.All(provided.Profiles, x => Assert.True(x.ReadOnly));
    }

    [Fact]
    public void AStoreCanOptOutOfTheBlanketReadOnly()
    {
        var provided = Build(
            ("ShinyDocDbMyAdmin:ReadOnly", "true"),
            ("ConnectionStrings:scratch", "Data Source=/tmp/scratch.db"),
            ("Shiny:DocumentDb:scratch:Provider", "Sqlite"),
            ("Shiny:DocumentDb:scratch:ReadOnly", "false"));

        Assert.False(Assert.Single(provided.Profiles).ReadOnly);
    }

    [Fact]
    public void SqlCipherPassword_ComesThroughSeparately()
    {
        var provided = Build(
            ("ConnectionStrings:secrets", "Data Source=/tmp/secrets.db"),
            ("Shiny:DocumentDb:secrets:Provider", "SqlCipher"),
            ("Shiny:DocumentDb:secrets:Password", "hunter2"));

        var profile = Assert.Single(provided.Profiles);
        Assert.Equal("hunter2", provided.Find(profile.Id)!.Password);
    }

    [Fact]
    public void ProfileIdsArePrefixed_SoTheyCannotCollideWithSavedOnes()
    {
        var provided = Build(
            ("ConnectionStrings:orders", "Data Source=/tmp/orders.db"),
            ("Shiny:DocumentDb:orders:Provider", "Sqlite"));

        Assert.Equal($"{ProvidedConnections.IdPrefix}orders", Assert.Single(provided.Profiles).Id);
    }

    [Fact]
    public void ProfilesAreSortedByName()
    {
        var provided = Build(
            ("ConnectionStrings:zulu", "Data Source=/tmp/z.db"),
            ("Shiny:DocumentDb:zulu:Provider", "Sqlite"),
            ("ConnectionStrings:alpha", "Data Source=/tmp/a.db"),
            ("Shiny:DocumentDb:alpha:Provider", "Sqlite"));

        Assert.Equal(["alpha", "zulu"], provided.Profiles.Select(x => x.Name));
    }

    [Fact]
    public void NothingConfigured_IsAnEmptyList_NotAFailure()
    {
        var provided = Build();

        Assert.Empty(provided.Profiles);
        Assert.False(provided.IsProvided("provided-anything"));
        Assert.Null(provided.Find("provided-anything"));
    }
}
