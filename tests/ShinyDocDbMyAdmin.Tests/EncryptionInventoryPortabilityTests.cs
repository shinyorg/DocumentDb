using System.Text.Json.Nodes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using Shiny.DocumentDb;
using Shiny.DocumentDb.PostgreSql;
using ShinyDocDbMyAdmin.Services;
using Testcontainers.PostgreSql;

namespace ShinyDocDbMyAdmin.Tests;

/// <summary>
/// The key inventory against a second dialect.
/// </summary>
/// <remarks>
/// <para>
/// Everything else about encryption in the admin tool is dialect-independent by construction — it reads a
/// JSON string out of a body it already fetched. The inventory is the exception: it counts in SQL, with
/// <c>LIKE</c> over <c>IDatabaseProvider.JsonExtract</c> and conditional aggregates. That form was chosen
/// over a portable <c>SUBSTRING</c> precisely because it is spelled the same in all nine dialects, and this
/// is the test that stops that from being an assumption.
/// </para>
/// <para>
/// PostgreSQL is the useful second one: its <c>#&gt;&gt;</c> extract returns text where SQLite's
/// <c>json_extract</c> returns a typed value, and its <c>LIKE</c> is case-sensitive where SQLite's is not.
/// </para>
/// </remarks>
public sealed class EncryptionInventoryPortabilityTests : IAsyncLifetime
{
    const string Table = "documents";
    const string TypeName = "Patient";

    PostgreSqlContainer container = null!;
    string directory = "";
    string profileId = "";
    DocumentAdminService admin = null!;
    ConnectionManager connections = null!;

    public async ValueTask InitializeAsync()
    {
        this.container = new PostgreSqlBuilder().Build();
        await this.container.StartAsync();

        await this.Seed();

        this.directory = Path.Combine(Path.GetTempPath(), $"docdbencpg_{Guid.NewGuid():N}");
        Directory.CreateDirectory(this.directory);

        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["ShinyDocDbMyAdmin:DataDirectory"] = this.directory,
                ["ShinyDocDbMyAdmin:SecretKey"] = "test-key",
                ["ConnectionStrings:store"] = this.container.GetConnectionString(),
                ["Shiny:DocumentDb:store:Provider"] = "PostgreSql"
            })
            .Build();

        var paths = new AppPaths(configuration);
        var protector = new SecretProtector(configuration, paths, NullLogger<SecretProtector>.Instance);
        var provided = new ProvidedConnections(configuration, NullLogger<ProvidedConnections>.Instance);
        var profiles = new ProfileStore(paths, protector, provided, new DemoMode(configuration, NullLogger<DemoMode>.Instance));

        this.profileId = provided.Profiles.Single().Id;
        this.connections = new ConnectionManager(profiles, NullLogger<ConnectionManager>.Instance);
        this.admin = new DocumentAdminService(this.connections, NullLogger<DocumentAdminService>.Instance);
    }

    /// <summary>
    /// Envelopes written verbatim through the schema-free lane rather than produced by an encryptor: what
    /// this test is about is the counting SQL, and hand-written values make the expected counts exact.
    /// </summary>
    async Task Seed()
    {
        using var store = new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new PostgreSqlDatabaseProvider(this.container.GetConnectionString()),
            TableName = Table
        });

        var patients = store.Collection(TypeName);

        for (var i = 0; i < 3; i++)
            await patients.Insert(Body($"k1_{i}", $"enc:1:k1:{Payload()}"));

        for (var i = 0; i < 2; i++)
            await patients.Insert(Body($"k2_{i}", $"enc:1:k2:{Payload()}"));

        // A key the sample will still see (only seven documents here), so this lands in ByKeyId rather than
        // in Other - the Other arithmetic is pinned on SQLite where the sample window can be forced.
        await patients.Insert(Body("plain", "555-55-5555"));

        // A key id carrying a LIKE metacharacter, which is the reason EscapeLikePattern is applied to the
        // prefix at all: unescaped, "key_2026" would also match "key12026".
        await patients.Insert(Body("underscored", $"enc:1:key_2026:{Payload()}"));
        await patients.Insert(Body("decoy", $"enc:1:keyX2026:{Payload()}"));
    }

    static JsonObject Body(string id, string ssn) => new() { ["id"] = id, ["ssn"] = ssn };

    static string Payload() => Convert.ToBase64String(Guid.NewGuid().ToByteArray());

    public async ValueTask DisposeAsync()
    {
        this.connections?.Dispose();
        if (this.container is not null)
            await this.container.DisposeAsync();

        if (Directory.Exists(this.directory))
        {
            try { Directory.Delete(this.directory, recursive: true); }
            catch (IOException) { /* not a test failure */ }
        }
    }

    [Fact]
    public async Task TheCountingSql_RunsOnPostgreSqlAndAgreesWithWhatWasWritten()
    {
        var inventory = await this.admin.DescribeEncryption(this.profileId, Table, TypeName);

        var ssn = Assert.Single(inventory.Paths);
        Assert.Equal(8L, ssn.Total);
        Assert.Equal(3L, ssn.ByKeyId["k1"]);
        Assert.Equal(2L, ssn.ByKeyId["k2"]);
        Assert.Equal(1L, ssn.Plaintext);
        Assert.Equal(0L, ssn.OtherKeys);
        Assert.Equal(ssn.Total, ssn.ByKeyId.Values.Sum() + ssn.OtherKeys + ssn.Plaintext);
    }

    [Fact]
    public async Task AKeyIdContainingAnUnderscore_DoesNotSwallowItsNeighbour()
    {
        // The bug this prevents: '_' is a LIKE metacharacter, so an unescaped 'enc:1:key_2026:%' would count
        // 'keyX2026' as well and quietly over-report the key an operator is about to retire.
        var ssn = (await this.admin.DescribeEncryption(this.profileId, Table, TypeName)).Paths.Single();

        Assert.Equal(1L, ssn.ByKeyId["key_2026"]);
        Assert.Equal(1L, ssn.ByKeyId["keyX2026"]);
    }

    [Fact]
    public async Task TheEnvelopesAreRecognisedTheSameWayTheyAreOnSqlite()
    {
        // The envelope lives in the JSON body on every backend, so detection has nothing dialect-specific
        // in it. Asserted here anyway, because "nothing dialect-specific" is the claim.
        var schema = await this.admin.InferSchema(this.profileId, Table, TypeName);
        var ssn = schema.Fields.Single(f => f.Path == "ssn");

        Assert.NotNull(ssn.Encryption);
        Assert.Equal(["k1", "k2", "keyX2026", "key_2026"], ssn.Encryption!.KeyIds.Order(StringComparer.Ordinal));
        Assert.Equal(1, ssn.Encryption.PlaintextCount);
        Assert.DoesNotContain("ssn", DocumentAdminService.SearchableColumns(schema));
    }
}
