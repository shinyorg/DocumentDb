using System.Security.Cryptography;
using System.Text.Json;
using Shiny.DocumentDb.Sqlite;
using Xunit;

namespace Shiny.DocumentDb.Tests;

// Field-level encryption is a serialization-level transform, so these run on SQLite: what they assert (envelope in
// the stored JSON, plaintext on materialization, predicate rewriting) is provider-independent by construction.
// The mapping registry is keyed by document type process-wide, so each scenario gets its own type.
public class EncryptionTests
{
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
        public int? Score { get; set; }
        public DateTime Taken { get; set; }
        public Guid Device { get; set; }
        public decimal Amount { get; set; }
    }

    public class LegacyDoc
    {
        public string Id { get; set; } = "";
        public string? Secret { get; set; }
    }

    public class Rotated
    {
        public string Id { get; set; } = "";
        public string? Secret { get; set; }
    }

    public class Unsupported
    {
        public string Id { get; set; } = "";
        public string[] Tags { get; set; } = [];
    }

    static readonly byte[] Key1 = AesGcmDocumentEncryptor.GenerateKey();
    static readonly byte[] Key2 = AesGcmDocumentEncryptor.GenerateKey();
    static readonly IDocumentEncryptor Encryptor = new AesGcmDocumentEncryptor("k1", Key1);

    static DocumentStore CreateStore(Action<DocumentStoreOptions> configure, string? connectionString = null)
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider(connectionString ?? "Data Source=:memory:"),
            TableName = $"t{Guid.NewGuid():N}"
        };
        configure(opts);
        return new DocumentStore(opts);
    }

    // The raw stored JSON, read through the schema-free lane so nothing deserializes (and so nothing decrypts).
    static async Task<string?> RawField(IDocumentStore store, Type documentType, string field)
    {
        var raw = await store.Collection(documentType).Query().ToList();
        return raw[0][field]?.GetValue<string>();
    }

    [Fact]
    public async Task Randomized_StoresCiphertext_AndReadsBackPlaintext()
    {
        using var store = CreateStore(o =>
        {
            o.UseEncryptor(Encryptor);
            o.ConfigureDocument<Patient>(cfg => cfg.MapProperty(x => x.Ssn, p => p.Encrypt()));
        });

        await store.Insert(new Patient { Id = "p1", Name = "Alice", Ssn = "123-45-6789" });

        var stored = await RawField(store, typeof(Patient), "ssn");
        Assert.StartsWith("enc:1:k1:", stored);
        Assert.DoesNotContain("123-45-6789", stored);

        // The unmapped property is untouched.
        Assert.Equal("Alice", await RawField(store, typeof(Patient), "name"));

        var loaded = await store.Get<Patient>("p1");
        Assert.Equal("123-45-6789", loaded!.Ssn);
    }

    [Fact]
    public async Task Randomized_ProducesDifferentCiphertextPerWrite_AndRejectsEqualityFilters()
    {
        using var store = CreateStore(o =>
        {
            o.UseEncryptor(Encryptor);
            o.ConfigureDocument<Patient>(cfg => cfg.MapProperty(x => x.Ssn, p => p.Encrypt()));
        });

        await store.Insert(new Patient { Id = "a", Name = "A", Ssn = "same" });
        await store.Insert(new Patient { Id = "b", Name = "B", Ssn = "same" });

        var raw = await store.Collection(typeof(Patient)).Query().OrderBy("id").ToList();
        Assert.NotEqual(raw[0]["ssn"]!.GetValue<string>(), raw[1]["ssn"]!.GetValue<string>());

        var ex = await Assert.ThrowsAsync<NotSupportedException>(() => store.Query<Patient>().Where(x => x.Ssn == "same").ToList());
        Assert.Contains("Deterministic", ex.Message);
    }

    [Fact]
    public async Task Deterministic_IsQueryableByEquality()
    {
        using var store = CreateStore(o =>
        {
            o.UseEncryptor(Encryptor);
            o.ConfigureDocument<Member>(cfg => cfg.MapProperty(x => x.Email, p => p.Encrypt(EncryptionMode.Deterministic)));
        });

        await store.Insert(new Member { Id = "m1", Email = "a@x.com", Region = "eu" });
        await store.Insert(new Member { Id = "m2", Email = "a@x.com", Region = "us" });
        await store.Insert(new Member { Id = "m3", Email = "b@x.com", Region = "eu" });

        var raw = await store.Collection(typeof(Member)).Query().OrderBy("id").ToList();
        Assert.Equal(raw[0]["email"]!.GetValue<string>(), raw[1]["email"]!.GetValue<string>());
        Assert.NotEqual(raw[0]["email"]!.GetValue<string>(), raw[2]["email"]!.GetValue<string>());

        // Typed LINQ, the string grammar and a combined predicate all lower to the same rewritten expression.
        Assert.Equal(2, await store.Query<Member>().Where(x => x.Email == "a@x.com").Count());
        Assert.Equal(2, await store.Query<Member>().Where("Email == 'a@x.com'").Count());
        Assert.Equal(1, await store.Query<Member>().Where(x => x.Email == "a@x.com" && x.Region == "eu").Count());
        Assert.Equal(1, await store.Query<Member>().Where(x => x.Email != "a@x.com").Count());

        var single = await store.Query<Member>().First(x => x.Email == "b@x.com");
        Assert.Equal("m3", single.Id);
    }

    [Fact]
    public async Task Deterministic_RejectsAnythingThatIsNotEquality()
    {
        using var store = CreateStore(o =>
        {
            o.UseEncryptor(Encryptor);
            o.ConfigureDocument<Member>(cfg => cfg.MapProperty(x => x.Email, p => p.Encrypt(EncryptionMode.Deterministic)));
        });

        await store.Insert(new Member { Id = "m1", Email = "a@x.com", Region = "eu" });

        await Assert.ThrowsAsync<NotSupportedException>(() => store.Query<Member>().Where(x => x.Email!.StartsWith("a")).ToList());
        await Assert.ThrowsAsync<NotSupportedException>(() => store.Query<Member>().Where(x => x.Email!.Contains("@")).ToList());
        await Assert.ThrowsAsync<NotSupportedException>(() =>
            store.Query<Member>().Where(x => String.Compare(x.Email, "b@x.com", StringComparison.Ordinal) < 0).ToList());
    }

    [Fact]
    public async Task NullValues_StayNullAndRemainQueryable()
    {
        using var store = CreateStore(o =>
        {
            o.UseEncryptor(Encryptor);
            o.ConfigureDocument<Member>(cfg => cfg.MapProperty(x => x.Email, p => p.Encrypt(EncryptionMode.Deterministic)));
        });

        await store.Insert(new Member { Id = "m1", Email = null, Region = "eu" });
        await store.Insert(new Member { Id = "m2", Email = "a@x.com", Region = "eu" });

        var loaded = await store.Get<Member>("m1");
        Assert.Null(loaded!.Email);

        Assert.Equal(1, await store.Query<Member>().Where(x => x.Email == null).Count());
        Assert.Equal(1, await store.Query<Member>().Where(x => x.Email != null).Count());
    }

    [Fact]
    public async Task NonStringProperties_RoundTripUnderRandomizedEncryption()
    {
        var taken = new DateTime(2026, 8, 4, 10, 30, 15, 123, DateTimeKind.Utc);
        var device = Guid.NewGuid();

        using var store = CreateStore(o =>
        {
            o.UseEncryptor(Encryptor);
            o.ConfigureDocument<Reading>(cfg =>
            {
                cfg.MapProperty(x => x.Score, p => p.Encrypt());
                cfg.MapProperty(x => x.Taken, p => p.Encrypt());
                cfg.MapProperty(x => x.Device, p => p.Encrypt());
                cfg.MapProperty(x => x.Amount, p => p.Encrypt());
            });
        });

        await store.Insert(new Reading { Id = "r1", Score = 42, Taken = taken, Device = device, Amount = 19.99m });

        Assert.StartsWith("enc:", await RawField(store, typeof(Reading), "score"));
        Assert.StartsWith("enc:", await RawField(store, typeof(Reading), "taken"));

        var loaded = await store.Get<Reading>("r1");
        Assert.Equal(42, loaded!.Score);
        Assert.Equal(taken, loaded.Taken);
        Assert.Equal(device, loaded.Device);
        Assert.Equal(19.99m, loaded.Amount);
    }

    [Fact]
    public void Mapping_RejectsUnsupportedTypesAndNonStringDeterministic()
    {
        var ex = Assert.Throws<ArgumentException>(() => CreateStore(o =>
        {
            o.UseEncryptor(Encryptor);
            o.ConfigureDocument<Unsupported>(cfg => cfg.MapProperty(x => x.Tags, p => p.Encrypt()));
        }));
        Assert.Contains("does not support", ex.Message);

        var deterministic = Assert.Throws<ArgumentException>(() => CreateStore(o =>
        {
            o.UseEncryptor(Encryptor);
            o.ConfigureDocument<Reading>(cfg => cfg.MapProperty(x => x.Score, p => p.Encrypt(EncryptionMode.Deterministic)));
        }));
        Assert.Contains("string properties only", deterministic.Message);
    }

    [Fact]
    public async Task PreEncryptionDocuments_StillRead_AndRewrapConvertsThem()
    {
        var file = Path.Combine(Path.GetTempPath(), $"enc{Guid.NewGuid():N}.db");
        var connection = $"Data Source={file}";
        var table = $"t{Guid.NewGuid():N}";
        try
        {
            // Written by a store that has no encryption configured — plaintext on disk.
            var plainOptions = new DocumentStoreOptions
            {
                DatabaseProvider = new SqliteDatabaseProvider(connection),
                TableName = table
            };
            using (var plain = new DocumentStore(plainOptions))
                await plain.Insert(new LegacyDoc { Id = "l1", Secret = "old-value" });

            var encryptedOptions = new DocumentStoreOptions
            {
                DatabaseProvider = new SqliteDatabaseProvider(connection),
                TableName = table
            };
            encryptedOptions.UseEncryptor(Encryptor);
            encryptedOptions.ConfigureDocument<LegacyDoc>(cfg => cfg.MapProperty(x => x.Secret, p => p.Encrypt()));

            using var store = new DocumentStore(encryptedOptions);

            // The pre-encryption row still reads.
            Assert.Equal("old-value", (await store.Get<LegacyDoc>("l1"))!.Secret);

            var rewrapped = await store.RewrapAsync<LegacyDoc>();
            Assert.Equal(1, rewrapped);

            Assert.StartsWith("enc:", await RawField(store, typeof(LegacyDoc), "secret"));
            Assert.Equal("old-value", (await store.Get<LegacyDoc>("l1"))!.Secret);
        }
        finally
        {
            SqliteTestFiles.Delete(file);
        }
    }

    [Fact]
    public async Task KeyRotation_ReadsOldEnvelopes_AndRewrapMovesThemToTheNewKey()
    {
        var encryptor = new RotatingEncryptor(new AesGcmDocumentEncryptor("k1", Key1));

        using var store = CreateStore(o =>
        {
            o.UseEncryptor(encryptor);
            o.ConfigureDocument<Rotated>(cfg => cfg.MapProperty(x => x.Secret, p => p.Encrypt()));
        });

        await store.Insert(new Rotated { Id = "r1", Secret = "value" });
        Assert.StartsWith("enc:1:k1:", await RawField(store, typeof(Rotated), "secret"));

        // Add the new key alongside the old one and make it current — old documents must still read.
        encryptor.Inner = new AesGcmDocumentEncryptor("k2", new Dictionary<string, byte[]>
        {
            ["k1"] = Key1,
            ["k2"] = Key2
        });
        Assert.Equal("value", (await store.Get<Rotated>("r1"))!.Secret);

        await store.RewrapAsync<Rotated>();
        Assert.StartsWith("enc:1:k2:", await RawField(store, typeof(Rotated), "secret"));

        // Only now is retiring the old key safe.
        encryptor.Inner = new AesGcmDocumentEncryptor("k2", Key2);
        Assert.Equal("value", (await store.Get<Rotated>("r1"))!.Secret);
    }

    [Fact]
    public void TamperedEnvelope_ThrowsInsteadOfReturningGarbage()
    {
        var envelope = Encryptor.Encrypt("sensitive"u8, EncryptionMode.Randomized);
        var parts = envelope.Split(':', 4);
        var payload = Convert.FromBase64String(parts[3]);
        payload[^1] ^= 0xFF;
        var tampered = $"{parts[0]}:{parts[1]}:{parts[2]}:{Convert.ToBase64String(payload)}";

        // AES-GCM reports a tag mismatch as a CryptographicException subclass, so match the family.
        Assert.ThrowsAny<CryptographicException>(() => Encryptor.Decrypt(tampered));
    }

    [Fact]
    public async Task PlaintextThatLooksLikeAnEnvelope_IsStillReadAsPlaintext()
    {
        using var store = CreateStore(o =>
        {
            o.UseEncryptor(Encryptor);
            o.ConfigureDocument<Patient>(cfg => cfg.MapProperty(x => x.Ssn, p => p.Encrypt()));
        });

        // Written through the raw lane so it bypasses the converter — a value a legacy row could plausibly hold.
        await store.Collection(typeof(Patient)).Insert(
            System.Text.Json.Nodes.JsonNode.Parse("""{"id":"weird","name":"N","ssn":"enc:not-a-version:x:y"}""")!.AsObject());

        Assert.Equal("enc:not-a-version:x:y", (await store.Get<Patient>("weird"))!.Ssn);
    }

    [Fact]
    public void UnknownKey_ExplainsItselfRatherThanFailingCryptically()
    {
        var written = new AesGcmDocumentEncryptor("retired", Key2).Encrypt("x"u8, EncryptionMode.Randomized);

        var ex = Assert.Throws<InvalidOperationException>(() => Encryptor.Decrypt(written));
        Assert.Contains("'retired'", ex.Message);
    }

    [Fact]
    public void DeterministicEncryption_IsStableAcrossEncryptorInstances()
    {
        var a = new AesGcmDocumentEncryptor("k1", Key1);
        var b = new AesGcmDocumentEncryptor("k1", Key1);

        Assert.Equal(
            a.Encrypt("value"u8, EncryptionMode.Deterministic),
            b.Encrypt("value"u8, EncryptionMode.Deterministic));

        // A different key must not produce the same ciphertext for the same plaintext.
        Assert.NotEqual(
            a.Encrypt("value"u8, EncryptionMode.Deterministic),
            new AesGcmDocumentEncryptor("k2", Key2).Encrypt("value"u8, EncryptionMode.Deterministic));
    }

    // A real deployment holds one encryptor whose key ring changes over time; the mapping pins the instance, not
    // the keys. This is that shape.
    sealed class RotatingEncryptor(IDocumentEncryptor inner) : IDocumentEncryptor
    {
        public IDocumentEncryptor Inner { get; set; } = inner;
        public string CurrentKeyId => this.Inner.CurrentKeyId;
        public string Encrypt(ReadOnlySpan<byte> plaintext, EncryptionMode mode) => this.Inner.Encrypt(plaintext, mode);
        public byte[] Decrypt(string envelope) => this.Inner.Decrypt(envelope);
    }

    static class SqliteTestFiles
    {
        public static void Delete(string file)
        {
            Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
            foreach (var path in new[] { file, file + "-wal", file + "-shm" })
            {
                if (File.Exists(path))
                    File.Delete(path);
            }
        }
    }
}
