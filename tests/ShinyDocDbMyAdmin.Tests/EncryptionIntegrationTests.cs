using System.Text.Json.Nodes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using Shiny.DocumentDb;
using Shiny.DocumentDb.Sqlite;
using ShinyDocDbMyAdmin.Models;
using ShinyDocDbMyAdmin.Providers;
using ShinyDocDbMyAdmin.Services;

namespace ShinyDocDbMyAdmin.Tests;

/// <summary>
/// The admin tool against documents a real <c>DocumentStore</c> encrypted.
/// </summary>
/// <remarks>
/// The test that matters here is not that the envelope parser works — that is pinned in the library's own
/// suite — but that what this tool reports about a type <b>agrees with what the store actually wrote</b>:
/// the same paths, the same key ids, the same counts. Everything else in this file exists because the admin
/// bypasses the library on write, so the guard against unprotecting a field has to be its own code.
/// </remarks>
public sealed class EncryptionIntegrationTests : IAsyncLifetime
{
    const string Table = "documents";

    // The mapping registry is keyed by document type process-wide, so these types are this file's alone.
    public class Patient
    {
        public string Id { get; set; } = "";
        public string Name { get; set; } = "";
        public string? Ssn { get; set; }
    }

    public class Member
    {
        public string Id { get; set; } = "";
        public string? Email { get; set; }
        public string Region { get; set; } = "";
    }

    public class Reading
    {
        public string Id { get; set; } = "";
        public string? Serial { get; set; }
        public int? Score { get; set; }
        public Guid Device { get; set; }
        public DateTime Taken { get; set; }
    }

    static readonly byte[] Key1 = AesGcmDocumentEncryptor.GenerateKey();
    static readonly byte[] Key2 = AesGcmDocumentEncryptor.GenerateKey();

    /// <summary>
    /// One encryptor instance for the whole file, because the mapping pins the instance rather than the
    /// keys — which is exactly the shape of a real deployment whose key ring changes over time. Rotation is
    /// modelled by swapping <see cref="RotatingEncryptor.Inner"/>, not by mapping a second encryptor.
    /// </summary>
    static readonly RotatingEncryptor Encryptor = new(new AesGcmDocumentEncryptor("k1", Key1));

    static readonly Dictionary<string, byte[]> BothKeys =
        new(StringComparer.Ordinal) { ["k1"] = Key1, ["k2"] = Key2 };

    string directory = "";
    string databasePath = "";
    string profileId = "";
    DocumentAdminService admin = null!;
    ConnectionManager connections = null!;
    ProfileStore profiles = null!;
    IConfiguration configuration = null!;

    public async ValueTask InitializeAsync()
    {
        this.directory = Path.Combine(Path.GetTempPath(), $"docdbenc_{Guid.NewGuid():N}");
        Directory.CreateDirectory(this.directory);
        this.databasePath = Path.Combine(this.directory, "store.db");

        await this.Seed();

        this.configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["ShinyDocDbMyAdmin:DataDirectory"] = Path.Combine(this.directory, "admin"),
                ["ShinyDocDbMyAdmin:SecretKey"] = "test-key",
                ["ConnectionStrings:store"] = $"Data Source={this.databasePath}",
                ["Shiny:DocumentDb:store:Provider"] = "Sqlite"
            })
            .Build();

        var paths = new AppPaths(this.configuration);
        var protector = new SecretProtector(this.configuration, paths, NullLogger<SecretProtector>.Instance);
        var provided = new ProvidedConnections(this.configuration, NullLogger<ProvidedConnections>.Instance);
        this.profiles = new ProfileStore(paths, protector, provided, this.Demo());

        this.profileId = provided.Profiles.Single().Id;
        this.connections = new ConnectionManager(this.profiles, NullLogger<ConnectionManager>.Instance);
        this.admin = new DocumentAdminService(this.connections, NullLogger<DocumentAdminService>.Instance);
    }

    /// <summary>
    /// Writes through the library, exactly as an application would. Everything the tool then reports has to
    /// line up with what this wrote.
    /// </summary>
    async Task Seed()
    {
        Encryptor.Inner = new AesGcmDocumentEncryptor("k1", BothKeys);

        using (var store = new DocumentStore(this.Options()))
        {
            await store.Insert(new Patient { Id = "p1", Name = "Ada", Ssn = "111-11-1111" });
            await store.Insert(new Patient { Id = "p2", Name = "Grace", Ssn = "222-22-2222" });
            await store.Insert(new Patient { Id = "p3", Name = "Alan", Ssn = "333-33-3333" });

            // Two members share an address. Deterministic mode makes that a repeated ciphertext, which is
            // the only thing that can prove the mode from stored data alone.
            await store.Insert(new Member { Id = "m1", Email = "shared@example.com", Region = "eu" });
            await store.Insert(new Member { Id = "m2", Email = "shared@example.com", Region = "us" });
            await store.Insert(new Member { Id = "m3", Email = "solo@example.com", Region = "eu" });

            await store.Insert(new Reading
            {
                Id = "r1",
                Serial = "SN-9000",
                Score = 42,
                Device = Guid.Parse("2f2c2a2e-0000-4000-8000-000000000002"),
                Taken = new DateTime(2026, 8, 4, 12, 0, 0, DateTimeKind.Utc)
            });
        }

        // A fourth patient written after the key ring rotated, so the inventory has something to report.
        Encryptor.Inner = new AesGcmDocumentEncryptor("k2", BothKeys);
        using (var store = new DocumentStore(this.Options()))
            await store.Insert(new Patient { Id = "p4", Name = "Edsger", Ssn = "444-44-4444" });

        // Back to k1 for anything a test writes afterwards, so the reveal tests only need the one key.
        Encryptor.Inner = new AesGcmDocumentEncryptor("k1", BothKeys);

        // …and one document with no envelope at all, standing in for one written before the property was
        // mapped. Through the schema-free lane, so nothing encrypts it on the way in.
        using (var store = new DocumentStore(this.Options()))
        {
            await store.Collection(nameof(Patient)).Insert(
                new JsonObject { ["id"] = "p5", ["name"] = "Barbara", ["ssn"] = "555-55-5555" });
        }
    }

    /// <summary>
    /// A store configured exactly as the application's would be.
    /// </summary>
    /// <remarks>
    /// The mappings are re-declared on <b>every</b> options instance, not just the first. The registry is
    /// process-wide and idempotent for an identical mapping, but the encrypting converters are installed as
    /// a <c>JsonTypeInfo</c> modifier on <i>these</i> serializer options - so a second store built without
    /// the declaration writes plaintext into a mapped property and nothing complains.
    /// </remarks>
    DocumentStoreOptions Options()
    {
        var options = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider($"Data Source={this.databasePath}"),
            TableName = Table
        };

        options.UseEncryptor(Encryptor);
        options.ConfigureDocument<Patient>(cfg => cfg.MapProperty(x => x.Ssn, p => p.Encrypt()));
        options.ConfigureDocument<Member>(cfg => cfg.MapProperty(x => x.Email, p => p.Encrypt(EncryptionMode.Deterministic)));
        options.ConfigureDocument<Reading>(cfg => cfg
            .MapProperty(x => x.Serial, p => p.Encrypt())
            .MapProperty(x => x.Score, p => p.Encrypt())
            .MapProperty(x => x.Device, p => p.Encrypt())
            .MapProperty(x => x.Taken, p => p.Encrypt()));

        return options;
    }

    DemoMode Demo(bool enabled = false)
    {
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?> { [DemoMode.ConfigurationKey] = enabled ? "true" : "false" })
            .Build();

        return new DemoMode(config, NullLogger<DemoMode>.Instance);
    }

    public ValueTask DisposeAsync()
    {
        this.connections?.Dispose();
        if (Directory.Exists(this.directory))
        {
            try { Directory.Delete(this.directory, recursive: true); }
            catch (IOException) { /* a driver still holding the file is not a test failure */ }
        }

        return ValueTask.CompletedTask;
    }

    // ── What the sample proves ──────────────────────────────────────────

    [Fact]
    public async Task TheInferredSchema_AgreesWithWhatTheStoreActuallyWrote()
    {
        var schema = await this.admin.InferSchema(this.profileId, Table, nameof(Patient));

        var ssn = schema.Fields.Single(f => f.Path == "ssn");
        Assert.NotNull(ssn.Encryption);
        Assert.Equal(["k1", "k2"], ssn.Encryption!.KeyIds.Order());
        Assert.Equal(4, ssn.Encryption.EncryptedCount);
        Assert.Equal(1, ssn.Encryption.PlaintextCount);

        // "encrypted" rather than "string": an envelope is a JSON string, but saying so describes the
        // storage rather than the field. The one plaintext straggler keeps "string" in the summary too.
        Assert.Contains("encrypted", ssn.Types);
        Assert.DoesNotContain("encrypted", schema.Fields.Single(f => f.Path == "name").Types);
    }

    [Fact]
    public async Task ARepeatedCiphertext_ProvesDeterministicMode()
    {
        var schema = await this.admin.InferSchema(this.profileId, Table, nameof(Member));

        Assert.True(schema.Fields.Single(f => f.Path == "email").Encryption!.DeterministicObserved);
    }

    [Fact]
    public async Task AllUniqueCiphertexts_ProveNothing_AndAreNeverReportedAsRandomized()
    {
        // The temptation this pins down: a deterministic column of distinct values is indistinguishable
        // from a randomized one, so "randomized" is unprovable and must never be claimed.
        var encryption = (await this.admin.InferSchema(this.profileId, Table, nameof(Patient)))
            .Fields.Single(f => f.Path == "ssn").Encryption!;

        Assert.False(encryption.DeterministicObserved);
        Assert.Equal("mode unknown", encryption.ModeSummary);
        Assert.DoesNotContain("randomi", encryption.ModeSummary, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task TheExample_SummarisesTheEnvelopeRatherThanShowingIt()
    {
        var serial = (await this.admin.InferSchema(this.profileId, Table, nameof(Reading)))
            .Fields.Single(f => f.Path == "serial");

        Assert.Contains("encrypted", serial.Example);
        Assert.DoesNotContain("enc:1:", serial.Example);
    }

    // ── Query surfaces that cannot work ─────────────────────────────────

    [Fact]
    public async Task QuickSearch_DoesNotCoverAnEncryptedPath()
    {
        // A LIKE over ciphertext is guaranteed noise, and a search that silently matches nothing reads as
        // "there is no such document".
        var schema = await this.admin.InferSchema(this.profileId, Table, nameof(Patient));

        Assert.DoesNotContain("ssn", DocumentAdminService.SearchableColumns(schema));
        Assert.Contains("name", DocumentAdminService.SearchableColumns(schema));
    }

    [Fact]
    public async Task TheDefaultColumns_LeaveEncryptedPathsOut_ButTheyStaySelectable()
    {
        var schema = await this.admin.InferSchema(this.profileId, Table, nameof(Patient));

        Assert.DoesNotContain("ssn", DocumentAdminService.DefaultColumns(schema));
        Assert.Contains("ssn", schema.Fields.Select(f => f.Path));
        Assert.Equal(["ssn"], DocumentAdminService.EncryptedPaths(schema));
    }

    [Fact]
    public async Task TheFilterConsole_WarnsThatAPredicateOverAnUnprovenModeCannotMatch()
    {
        var warnings = await this.admin.EncryptionWarnings(
            this.profileId, Table, nameof(Patient), new FilterQuery(Where: "ssn == '111-11-1111'"));

        var warning = Assert.Single(warnings);
        Assert.Contains("cannot match", warning);
        Assert.Contains("compiles to SQL", warning);
    }

    [Fact]
    public async Task TheFilterConsole_TellsADeterministicPathApart()
    {
        var warnings = await this.admin.EncryptionWarnings(
            this.profileId, Table, nameof(Member), new FilterQuery(Where: "email == 'shared@example.com'"));

        Assert.Contains("exact-ciphertext equality", Assert.Single(warnings));
    }

    [Fact]
    public async Task AQueryThatTouchesNothingProtected_WarnsAboutNothing()
    {
        var warnings = await this.admin.EncryptionWarnings(
            this.profileId, Table, nameof(Patient), new FilterQuery(Where: "name == 'Ada'"));

        Assert.Empty(warnings);
    }

    [Fact]
    public async Task IndexingAPathOfUnprovenMode_IsWarnedAbout_NotBlocked()
    {
        var schema = await this.admin.InferSchema(this.profileId, Table, nameof(Patient));
        var warning = DocumentAdminService.IndexWarningFor(schema, ["ssn"]);

        Assert.NotNull(warning);
        Assert.Contains("dead weight", warning);

        // Warned about, and then created anyway - the tool cannot prove the mode, so it must not refuse.
        await this.admin.CreateIndex(this.profileId, Table, nameof(Patient), "ssn");
        Assert.Contains(await this.admin.ListIndexes(this.profileId, Table), i => i.Paths.Contains("ssn"));
    }

    [Fact]
    public async Task IndexingADeterministicPath_IsEncouraged()
    {
        var schema = await this.admin.InferSchema(this.profileId, Table, nameof(Member));

        Assert.Contains("does work", DocumentAdminService.IndexWarningFor(schema, ["email"]));
    }

    [Fact]
    public async Task IndexingAnOrdinaryPath_SaysNothingAboutEncryption()
    {
        var schema = await this.admin.InferSchema(this.profileId, Table, nameof(Patient));

        Assert.Null(DocumentAdminService.IndexWarningFor(schema, ["name"]));
    }

    // ── The inventory ───────────────────────────────────────────────────

    [Fact]
    public async Task TheInventory_CountsEveryDocument_NotJustTheSample()
    {
        var inventory = await this.admin.DescribeEncryption(this.profileId, Table, nameof(Patient));

        var ssn = Assert.Single(inventory.Paths);
        Assert.Equal("ssn", ssn.Path);
        Assert.Equal(5L, ssn.Total);
        Assert.Equal(3L, ssn.ByKeyId["k1"]);
        Assert.Equal(1L, ssn.ByKeyId["k2"]);
        Assert.Equal(1L, ssn.Plaintext);
        Assert.Equal(0L, ssn.OtherKeys);
        Assert.False(ssn.RotationComplete);
    }

    [Fact]
    public async Task TheVerdict_LeadsWithThePlaintextStragglers()
    {
        var ssn = (await this.admin.DescribeEncryption(this.profileId, Table, nameof(Patient))).Paths.Single();

        Assert.Contains("not encrypted", ssn.Verdict);
        Assert.Contains("RewrapAsync", ssn.Verdict);
    }

    [Fact]
    public async Task ARotationWithNothingLeftBehind_ReportsAsComplete()
    {
        var inventory = await this.admin.DescribeEncryption(this.profileId, Table, nameof(Member));

        var email = Assert.Single(inventory.Paths);
        Assert.True(email.RotationComplete);
        Assert.Equal(3L, email.ByKeyId["k1"]);
        Assert.Contains("Nothing left under an older key", email.Verdict);
        Assert.True(inventory.RotationComplete);
    }

    [Fact]
    public async Task WhenTwoKeysAreInPlay_TheVerdictNamesTheOneToRewrapOff()
    {
        // Remove the plaintext straggler so the rotation verdict is the one that surfaces.
        await this.admin.DeleteDocuments(this.profileId, Table, nameof(Patient), ["p5"]);

        var ssn = (await this.admin.DescribeEncryption(this.profileId, Table, nameof(Patient))).Paths.Single();

        Assert.Equal(0L, ssn.Plaintext);
        Assert.Equal("k1", ssn.DominantKeyId);
        Assert.Contains("still under k2", ssn.Verdict);
        Assert.Contains("RewrapAsync<T>()", ssn.Verdict);
    }

    [Fact]
    public async Task AnEnvelopeUnderAKeyTheSampleNeverSaw_LandsInTheOtherBucket()
    {
        // The sample reads the newest 200 documents, so a key that only the oldest document uses is
        // genuinely invisible to it. The Other bucket is what stops that from being silently miscounted.
        using (var store = new DocumentStore(this.Options()))
        {
            var legacy = store.Collection("Legacy");
            await legacy.Insert(new JsonObject { ["id"] = "aged", ["secret"] = "enc:1:k9:AAECAwQFBgcICQoL" });

            for (var i = 0; i < DocumentAdminService.SchemaSampleSize + 10; i++)
            {
                await legacy.Insert(new JsonObject
                {
                    ["id"] = $"n{i:D4}",
                    ["secret"] = $"enc:1:k1:{Convert.ToBase64String(Guid.NewGuid().ToByteArray())}"
                });
            }
        }

        var schema = await this.admin.InferSchema(this.profileId, Table, "Legacy");
        Assert.Equal(["k1"], schema.Fields.Single(f => f.Path == "secret").Encryption!.KeyIds);

        var stats = (await this.admin.DescribeEncryption(this.profileId, Table, "Legacy")).Paths.Single();

        Assert.Equal(DocumentAdminService.SchemaSampleSize + 11L, stats.Total);
        Assert.Equal(0L, stats.Plaintext);
        Assert.Equal(1L, stats.OtherKeys);
        Assert.Contains("not seen in the sample", stats.Verdict);

        // The arithmetic that makes the bucket self-correcting: it is always the remainder.
        Assert.Equal(stats.Total, stats.ByKeyId.Values.Sum() + stats.OtherKeys + stats.Plaintext);
    }

    [Fact]
    public async Task ATypeWithNothingEncrypted_HasAnEmptyInventory()
    {
        using (var store = new DocumentStore(this.Options()))
            await store.Collection("Plain").Insert(new JsonObject { ["id"] = "x1", ["value"] = "nothing secret" });

        Assert.True((await this.admin.DescribeEncryption(this.profileId, Table, "Plain")).IsEmpty);
    }

    // ── The write guard ─────────────────────────────────────────────────

    [Fact]
    public async Task SavingPlaintextOverAnEnvelope_Throws()
    {
        var ex = await Assert.ThrowsAsync<EncryptedFieldDowngradeException>(() => this.admin.SaveDocument(
            this.profileId, Table, nameof(Patient), "p1",
            """{"id":"p1","name":"Ada","ssn":"111-11-1111"}""",
            isNew: false));

        Assert.Equal(["ssn"], ex.Paths);

        // And it really did not write.
        var stored = await this.admin.GetDocument(this.profileId, Table, nameof(Patient), "p1");
        Assert.True(EncryptedFields.TryRead(stored!.Read("ssn"), out _));
    }

    [Fact]
    public async Task SavingPlaintextOverAnEnvelope_IsAllowedWhenTheCallerSaysSo()
    {
        await this.admin.SaveDocument(
            this.profileId, Table, nameof(Patient), "p1",
            """{"id":"p1","name":"Ada","ssn":"111-11-1111"}""",
            isNew: false, allowEncryptionDowngrade: true);

        var stored = await this.admin.GetDocument(this.profileId, Table, nameof(Patient), "p1");
        Assert.Equal("111-11-1111", stored!.Read("ssn")!.GetValue<string>());
    }

    [Fact]
    public async Task EditingSomethingElse_SavesCleanly()
    {
        var before = await this.admin.GetDocument(this.profileId, Table, nameof(Patient), "p2");
        var envelope = before!.Read("ssn")!.GetValue<string>();

        await this.admin.SaveDocument(
            this.profileId, Table, nameof(Patient), "p2",
            $$"""{"id":"p2","name":"Grace Hopper","ssn":{{System.Text.Json.JsonSerializer.Serialize(envelope)}}}""",
            isNew: false);

        var after = await this.admin.GetDocument(this.profileId, Table, nameof(Patient), "p2");
        Assert.Equal("Grace Hopper", after!.Read("name")!.GetValue<string>());
        Assert.Equal(envelope, after.Read("ssn")!.GetValue<string>());
    }

    [Fact]
    public async Task RemovingAProtectedFieldEntirely_IsNotADowngrade()
    {
        await this.admin.SaveDocument(
            this.profileId, Table, nameof(Patient), "p3",
            """{"id":"p3","name":"Alan"}""",
            isNew: false);

        var stored = await this.admin.GetDocument(this.profileId, Table, nameof(Patient), "p3");
        Assert.Null(stored!.Read("ssn"));
    }

    [Fact]
    public async Task ANewDocumentWithPlaintextInAProtectedPath_IsRefusedToo()
    {
        var ex = await Assert.ThrowsAsync<EncryptedFieldDowngradeException>(() => this.admin.SaveDocument(
            this.profileId, Table, nameof(Patient), "p99",
            """{"id":"p99","name":"New","ssn":"999-99-9999"}""",
            isNew: true));

        Assert.Equal(["ssn"], ex.Paths);
    }

    [Fact]
    public async Task ANewDocumentThatSimplyOmitsTheProtectedField_SavesCleanly()
    {
        await this.admin.SaveDocument(
            this.profileId, Table, nameof(Patient), "p98",
            """{"id":"p98","name":"No SSN on file"}""",
            isNew: true);

        Assert.NotNull(await this.admin.GetDocument(this.profileId, Table, nameof(Patient), "p98"));
    }

    [Fact]
    public async Task ImportingPlaintextIntoAProtectedPath_IsCountedRatherThanRefused()
    {
        var transfers = new ImportExportService(this.admin, this.Demo(), NullLogger<ImportExportService>.Instance);

        var ndjson = string.Join('\n',
            """{"id":"i1","name":"One","ssn":"111-00-0001"}""",
            """{"id":"i2","name":"Two","ssn":"111-00-0002"}""",
            """{"id":"i3","name":"Three"}""");

        using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(ndjson));
        var result = await transfers.Import(this.profileId, Table, nameof(Patient), stream, ImportMode.Replace);

        // Blocking the whole file would be the wrong response; saying nothing would be worse.
        Assert.Equal(3, result.Written);
        Assert.Equal(2, result.EncryptionDowngrades);
        Assert.Contains("no longer protected", result.EncryptionWarning);
    }

    [Fact]
    public async Task AnImportThatTouchesNothingProtected_SaysNothing()
    {
        var transfers = new ImportExportService(this.admin, this.Demo(), NullLogger<ImportExportService>.Instance);

        using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes("""{"id":"i4","name":"Four"}"""));
        var result = await transfers.Import(this.profileId, Table, nameof(Patient), stream, ImportMode.Replace);

        Assert.Equal(0, result.EncryptionDowngrades);
        Assert.Null(result.EncryptionWarning);
    }

    [Fact]
    public async Task AnExportCarriesTheCiphertextVerbatim()
    {
        // "Export" reads as "a readable file", and for a protected field it is not. What it must be is
        // faithful: the envelope leaves exactly as stored, so a restore puts back something readable.
        var transfers = new ImportExportService(this.admin, this.Demo(), NullLogger<ImportExportService>.Instance);

        using var buffer = new MemoryStream();
        await transfers.Export(this.profileId, Table, nameof(Patient), ExportFormat.Ndjson, new BrowseQuery(), buffer);

        var text = System.Text.Encoding.UTF8.GetString(buffer.ToArray());
        Assert.Contains("enc:1:k1:", text);
        Assert.DoesNotContain("111-11-1111", text);
    }

    // ── Reading protected values (Phase 5) ──────────────────────────────

    /// <summary>
    /// A saved profile carrying key material. Host-provided connections deliberately carry none — their
    /// secrets come from the host and there is nowhere for an operator to have typed a data key.
    /// </summary>
    async Task<string> SaveKeyedProfile(params (string Id, byte[] Key)[] keys)
    {
        var profile = new ConnectionProfile
        {
            Name = $"keyed-{Guid.NewGuid():N}",
            Provider = ProviderKind.Sqlite,
            EncryptionKeys = [.. keys.Select(k => new EncryptionKeyEntry
            {
                KeyId = k.Id,
                Key = Convert.ToBase64String(k.Key)
            })]
        };

        await this.profiles.Save(profile, $"Data Source={this.databasePath}", null);
        return profile.Id;
    }

    EncryptionKeyRing Ring(DemoMode? demo = null)
        => new(this.profiles, demo ?? this.Demo(), NullLogger<EncryptionKeyRing>.Instance);

    async Task<string> StoredEnvelope(string typeName, string id, string path)
    {
        var row = await this.admin.GetDocument(this.profileId, Table, typeName, id);
        return row!.Read(path)!.GetValue<string>();
    }

    [Fact]
    public async Task WithTheRightKey_AValueComesBack()
    {
        var keyed = await this.SaveKeyedProfile(("k1", Key1));

        var result = await this.Ring().Reveal(keyed, await this.StoredEnvelope(nameof(Patient), "p1", "ssn"));

        Assert.True(result.Succeeded);
        Assert.Equal("111-11-1111", result.Text);
    }

    [Fact]
    public async Task EveryCodecRendersWithoutKnowingTheClrType()
    {
        // The finding the whole reveal lane rests on: ValueCodec encodes as UTF-8 text, so decryption needs
        // the key and not the property's type. A string, an int, a Guid and a DateTime all come back readable
        // without the tool ever being told which is which.
        var keyed = await this.SaveKeyedProfile(("k1", Key1));
        var ring = this.Ring();

        Assert.Equal("SN-9000", (await ring.Reveal(keyed, await this.StoredEnvelope(nameof(Reading), "r1", "serial"))).Text);
        Assert.Equal("42", (await ring.Reveal(keyed, await this.StoredEnvelope(nameof(Reading), "r1", "score"))).Text);
        Assert.Equal(
            "2f2c2a2e-0000-4000-8000-000000000002",
            (await ring.Reveal(keyed, await this.StoredEnvelope(nameof(Reading), "r1", "device"))).Text);
        Assert.Contains("2026-08-04", (await ring.Reveal(keyed, await this.StoredEnvelope(nameof(Reading), "r1", "taken"))).Text);
    }

    [Fact]
    public async Task TheWrongKeyUnderTheRightId_ReportsThatItWouldNotAuthenticate()
    {
        // AES-GCM authenticates, so this is either tampering or the wrong key material - and the envelope
        // cannot tell those apart, which is what the message has to say.
        var keyed = await this.SaveKeyedProfile(("k1", AesGcmDocumentEncryptor.GenerateKey()));

        var result = await this.Ring().Reveal(keyed, await this.StoredEnvelope(nameof(Patient), "p1", "ssn"));

        Assert.Equal(DecryptOutcome.Tampered, result.Outcome);
        Assert.Contains("altered", result.Message);
    }

    [Fact]
    public async Task AKeyThatIsNotHeld_IsNamed()
    {
        var keyed = await this.SaveKeyedProfile(("k1", Key1));

        // p4 was written after the ring rotated, under k2, which this connection does not carry.
        var result = await this.Ring().Reveal(keyed, await this.StoredEnvelope(nameof(Patient), "p4", "ssn"));

        Assert.Equal(DecryptOutcome.KeyNotHeld, result.Outcome);
        Assert.Contains("k2", result.Message);
    }

    [Fact]
    public async Task NoKeysAtAll_SaysWhichKeyWouldBeNeeded()
    {
        var keyed = await this.SaveKeyedProfile();
        var ring = this.Ring();

        var result = await ring.Reveal(keyed, await this.StoredEnvelope(nameof(Patient), "p1", "ssn"));

        Assert.Equal(DecryptOutcome.NotConfigured, result.Outcome);
        Assert.Contains("k1", result.Message);
        Assert.False(await ring.IsAvailable(keyed));
    }

    [Fact]
    public async Task AHostProvidedConnection_NeverCarriesKeys()
    {
        // Its secrets come from the AppHost and only ever live in memory; there is nowhere an operator
        // could have typed a data key, and pretending otherwise would make the reveal button lie.
        Assert.False(await this.Ring().IsAvailable(this.profileId));
    }

    [Fact]
    public async Task AMistypedKey_ReadsAsAConfigurationProblem_NotADataProblem()
    {
        var profile = new ConnectionProfile
        {
            Name = "bad key",
            Provider = ProviderKind.Sqlite,
            EncryptionKeys = [new EncryptionKeyEntry { KeyId = "k1", Key = "this-is-not-a-base64-aes-256-key" }]
        };
        await this.profiles.Save(profile, $"Data Source={this.databasePath}", null);

        var result = await this.Ring().Reveal(profile.Id, await this.StoredEnvelope(nameof(Patient), "p1", "ssn"));

        Assert.Equal(DecryptOutcome.UnusableKey, result.Outcome);
    }

    [Fact]
    public async Task DemoMode_RefusesToReadProtectedValues()
    {
        // A public playground is the one place storing data keys is never worth it, and the refusal lives
        // here as well as in the connection editor - so a profile seeded some other way still will not read.
        var keyed = await this.SaveKeyedProfile(("k1", Key1));
        var ring = this.Ring(this.Demo(enabled: true));

        var result = await ring.Reveal(keyed, await this.StoredEnvelope(nameof(Patient), "p1", "ssn"));

        Assert.Equal(DecryptOutcome.Refused, result.Outcome);
        Assert.False(await ring.IsAvailable(keyed));
        Assert.Empty(await ring.HeldKeyIds(keyed));
    }

    [Fact]
    public async Task AValueThatIsNotAnEnvelope_IsSaidToBeOne_RatherThanFailingAsADecrypt()
    {
        var keyed = await this.SaveKeyedProfile(("k1", Key1));

        Assert.Equal(DecryptOutcome.NotEncrypted, (await this.Ring().Reveal(keyed, "Ada")).Outcome);
    }

    [Fact]
    public async Task KeysRoundTripThroughTheProfileStore_AndAreNotStoredInTheClear()
    {
        var keyed = await this.SaveKeyedProfile(("k1", Key1), ("k2", Key2));

        var stored = await this.profiles.Get(keyed);
        Assert.All(stored!.EncryptionKeys, k => Assert.StartsWith("enc:v1:", k.Key));

        Assert.Equal(["k1", "k2"], (await this.Ring().HeldKeyIds(keyed)).Order());
        Assert.Equal(
            [Convert.ToBase64String(Key1), Convert.ToBase64String(Key2)],
            this.profiles.RevealEncryptionKeys(stored).Select(k => k.Key));
    }

    [Fact]
    public async Task ABlankKeyEntry_IsDroppedRatherThanStored()
    {
        // A key ring with an empty slot fails at read time with nothing to point at.
        var profile = new ConnectionProfile
        {
            Name = "half filled",
            Provider = ProviderKind.Sqlite,
            EncryptionKeys =
            [
                new EncryptionKeyEntry { KeyId = "k1", Key = Convert.ToBase64String(Key1) },
                new EncryptionKeyEntry { KeyId = "", Key = "" }
            ]
        };

        await this.profiles.Save(profile, $"Data Source={this.databasePath}", null);

        Assert.Equal("k1", Assert.Single((await this.profiles.Get(profile.Id))!.EncryptionKeys).KeyId);
    }

    [Fact]
    public void TheAssistantHasNoDecryptingTool()
    {
        // Not a rule the model is asked to follow: the function does not exist. A decrypted value would be
        // posted to a third-party model endpoint, which is the whole argument.
        Assert.DoesNotContain(AiToolSurface.ToolNames, n => n.Contains("decrypt", StringComparison.OrdinalIgnoreCase));
        Assert.DoesNotContain(AiToolSurface.ToolNames, n => n.Contains("reveal", StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>
    /// A real deployment holds one encryptor whose key ring changes over time; the mapping pins the
    /// instance, not the keys. This is that shape, and it is the only way to write a document under a
    /// second key from a process that already mapped the property.
    /// </summary>
    sealed class RotatingEncryptor(IDocumentEncryptor inner) : IDocumentEncryptor
    {
        public IDocumentEncryptor Inner { get; set; } = inner;
        public string CurrentKeyId => this.Inner.CurrentKeyId;
        public string Encrypt(ReadOnlySpan<byte> plaintext, EncryptionMode mode) => this.Inner.Encrypt(plaintext, mode);
        public byte[] Decrypt(string envelope) => this.Inner.Decrypt(envelope);
    }
}
