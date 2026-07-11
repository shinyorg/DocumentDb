using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Data.Sqlite;
using Shiny.DocumentDb.Sqlite;
using Xunit;

namespace Shiny.DocumentDb.Tests;

// Regression for DEFERRED-BUGS #2: the version / spatial / vector / computed JSON-path resolvers used to compute
// the stored name from the runtime naming policy (jsonOptions.PropertyNamingPolicy.ConvertName). The resolved
// JsonTypeInfo bakes each property's effective JSON name — honoring [JsonPropertyName] and source-gen contexts —
// which the naming policy cannot reproduce. When the two disagree the resolvers targeted a key that isn't in the
// document: the version CAS predicate read a missing path and every optimistic-concurrency write failed.
//
// A [JsonPropertyName] override is the cleanest reproduction because the baked name ("ver") is something the
// naming policy will never derive from the CLR name ("Version"), regardless of whether the store uses the
// reflection resolver or a source-generated context.
public class JsonContextPathResolverTests : IDisposable
{
    public sealed class NamedVersionDoc
    {
        public string Id { get; set; } = "";
        public int Age { get; set; }

        [JsonPropertyName("ver")]
        public int Version { get; set; }
    }

    readonly SqliteConnection holdOpen;
    readonly string connectionString;

    public JsonContextPathResolverTests()
    {
        this.connectionString = $"Data Source=jc_{Guid.NewGuid():N};Mode=Memory;Cache=Shared";
        this.holdOpen = new SqliteConnection(this.connectionString);
        this.holdOpen.Open();
    }

    public void Dispose() => this.holdOpen.Dispose();

    DocumentStore CreateStore(Action<DocumentStoreOptions> configure)
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider(this.connectionString),
            TableName = $"t{Guid.NewGuid():N}"
        };
        configure(opts);
        return new DocumentStore(opts);
    }

    [Fact]
    public async Task VersionCas_Works_When_JsonPropertyNameDiffersFromPolicy()
    {
        using var store = this.CreateStore(o => o.MapVersionProperty<NamedVersionDoc>(d => d.Version));

        var doc = new NamedVersionDoc { Id = "u1", Age = 30 };
        await store.Insert(doc);
        Assert.Equal(1, doc.Version);

        // The document stores the version under "ver" (the [JsonPropertyName]); the CAS predicate must read that
        // key, not the policy-derived "version". With the old resolver JsonExtract(Data, "version") returned NULL,
        // so even this first, in-order update was rejected as a phantom conflict.
        var first = await store.Get<NamedVersionDoc>("u1");
        var second = await store.Get<NamedVersionDoc>("u1");

        first!.Age = 31;
        await store.Update(first);
        Assert.Equal(2, first.Version);

        // A genuinely stale writer must still be rejected.
        second!.Age = 99;
        await Assert.ThrowsAsync<ConcurrencyException>(async () => await store.Update(second));

        var got = await store.Get<NamedVersionDoc>("u1");
        Assert.Equal(31, got!.Age);
        Assert.Equal(2, got.Version);
    }
}
