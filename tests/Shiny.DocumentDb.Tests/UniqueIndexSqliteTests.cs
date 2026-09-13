using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Microsoft.Data.Sqlite;
using Shiny.DocumentDb.Sqlite;
using Xunit;

namespace Shiny.DocumentDb.Tests;

// Unique-index behavior only a relational store has — tenancy, units of work and explicit transactions, index DDL over
// existing data — plus the mapping rules and the provider-agnostic key helper, run on SQLite. The behavior every
// provider shares is asserted in UniqueIndexConformanceTests. Encryption mappings are keyed by type process-wide, so the
// encryption scenarios each get their own type.
public class UniqueIndexSqliteTests
{
    public class UqTenantUser
    {
        public string Id { get; set; } = "";
        public string? Email { get; set; }
    }

    public class UqTableUser
    {
        public string Id { get; set; } = "";
        public string? Email { get; set; }
    }

    public class UqTxUser
    {
        public string Id { get; set; } = "";
        public string? Email { get; set; }
    }

    public class UqDupUser
    {
        public string Id { get; set; } = "";
        public string? Email { get; set; }
    }

    public class UqRandomizedUser
    {
        public string Id { get; set; } = "";
        public string? Email { get; set; }
    }

    public class UqDeterministicUser
    {
        public string Id { get; set; } = "";
        public string? Email { get; set; }
    }

    public class UqKeyDoc
    {
        public string Id { get; set; } = "";
        public string? Email { get; set; }
        public string? Region { get; set; }
        public bool IsDeleted { get; set; }
        public decimal? Amount { get; set; }
    }

    static DocumentStore CreateStore(Action<DocumentStoreOptions> configure, string connectionString = "Data Source=:memory:")
    {
        var opts = new DocumentStoreOptions { DatabaseProvider = new SqliteDatabaseProvider(connectionString) };
        configure(opts);
        return new DocumentStore(opts);
    }

    static DocumentMappingRegistry MappingsOf(Action<DocumentStoreOptions> configure)
    {
        var opts = new DocumentStoreOptions { DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:") };
        configure(opts);
        return ((IDocumentStoreOptions)opts).Mappings;
    }

    [Fact]
    public async Task Tenancy_TheIndexIsScopedToTheTenant()
    {
        var tenant = "t1";
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:"),
            TenantIdAccessor = () => tenant
        };
        opts.ConfigureDocument<UqTenantUser>(cfg => cfg.MapUniqueIndex(x => x.Email));
        using var store = new DocumentStore(opts);

        await store.Insert(new UqTenantUser { Id = "u1", Email = "a@x.com" });
        tenant = "t2";
        await store.Insert(new UqTenantUser { Id = "u2", Email = "a@x.com" });

        await Assert.ThrowsAsync<UniqueConstraintException>(() => store.Insert(new UqTenantUser { Id = "u3", Email = "a@x.com" }));
    }

    [Fact]
    public async Task CustomTable_IsIndexedWhenFirstWrittenInsideAUnitOfWork()
    {
        using var store = CreateStore(o => o.ConfigureDocument<UqTableUser>(cfg =>
        {
            cfg.Table = "uq_table_users";
            cfg.MapUniqueIndex(x => x.Email);
        }));

        await using (var session = store.OpenSession())
        {
            session.Add(new UqTableUser { Id = "u1", Email = "a@x.com" });
            await session.SaveChanges();
        }

        await Assert.ThrowsAsync<UniqueConstraintException>(() => store.Insert(new UqTableUser { Id = "u2", Email = "a@x.com" }));
    }

    [Fact]
    public async Task SaveChanges_WithADuplicate_RollsBackTheWholeUnit()
    {
        using var store = CreateStore(o => o.ConfigureDocument<UqTxUser>(cfg => cfg.MapUniqueIndex(x => x.Email)));

        await using var session = store.OpenSession();
        session.Add(new UqTxUser { Id = "u1", Email = "a@x.com" });
        session.Add(new UqTxUser { Id = "u2", Email = "b@x.com" });
        session.Add(new UqTxUser { Id = "u3", Email = "a@x.com" });

        await Assert.ThrowsAsync<UniqueConstraintException>(() => session.SaveChanges());
        Assert.Equal(0, await store.Query<UqTxUser>().Count());
    }

    [Fact]
    public async Task ExplicitTransaction_TranslatesTheViolation()
    {
        using var store = CreateStore(o => o.ConfigureDocument<UqTxUser>(cfg => cfg.MapUniqueIndex(x => x.Email)));
        await store.Insert(new UqTxUser { Id = "u1", Email = "a@x.com" });
        await store.Insert(new UqTxUser { Id = "u2", Email = "b@x.com" });

        await using var session = store.OpenSession();
        await using var tx = await session.BeginTransaction();

        session.Update(new UqTxUser { Id = "u2", Email = "a@x.com" });
        var ex = await Assert.ThrowsAsync<UniqueConstraintException>(() => session.SaveChanges());
        Assert.Equal("u2", ex.DocumentId);
        session.ClearPending();

        await Assert.ThrowsAsync<UniqueConstraintException>(() =>
            session.Query<UqTxUser>().Where(x => x.Id == "u2").ExecuteUpdate(x => x.Email!, "a@x.com"));

        await tx.Rollback();
        Assert.Equal("b@x.com", (await store.Get<UqTxUser>("u2"))!.Email);
    }

    [Fact]
    public async Task ExistingDuplicates_FailIndexCreationWithAConfigurationError()
    {
        var file = Path.Combine(Path.GetTempPath(), $"uq_{Guid.NewGuid():N}.db");
        try
        {
            using (var plain = CreateStore(_ => { }, $"Data Source={file}"))
            {
                await plain.Insert(new UqDupUser { Id = "u1", Email = "a@x.com" });
                await plain.Insert(new UqDupUser { Id = "u2", Email = "a@x.com" });
            }

            using var store = CreateStore(o => o.ConfigureDocument<UqDupUser>(cfg => cfg.MapUniqueIndex(x => x.Email)), $"Data Source={file}");
            var ex = await Assert.ThrowsAsync<DocumentConfigurationException>(() => store.Insert(new UqDupUser { Id = "u3", Email = "c@x.com" }));
            Assert.Contains("uq_UqDupUser_Email", ex.Message);
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            foreach (var path in new[] { file, file + "-wal", file + "-shm" })
                File.Delete(path);
        }
    }

    [Fact]
    public void RandomizedEncryptedKey_IsRejectedWhenTheStoreIsBuilt()
    {
        var ex = Assert.Throws<DocumentConfigurationException>(() => CreateStore(o =>
        {
            o.UseEncryptor(new AesGcmDocumentEncryptor("k1", AesGcmDocumentEncryptor.GenerateKey()));
            o.ConfigureDocument<UqRandomizedUser>(cfg =>
            {
                cfg.MapProperty(x => x.Email, p => p.Encrypt());
                cfg.MapUniqueIndex(x => x.Email);
            });
        }));

        Assert.Contains("randomized-encrypted", ex.Message);
    }

    [Fact]
    public async Task DeterministicEncryptedKey_IsEnforced()
    {
        using var store = CreateStore(o =>
        {
            o.UseEncryptor(new AesGcmDocumentEncryptor("k1", AesGcmDocumentEncryptor.GenerateKey()));
            o.ConfigureDocument<UqDeterministicUser>(cfg =>
            {
                cfg.MapProperty(x => x.Email, p => p.Encrypt(EncryptionMode.Deterministic));
                cfg.MapUniqueIndex(x => x.Email);
            });
        });

        await store.Insert(new UqDeterministicUser { Id = "u1", Email = "a@x.com" });
        await Assert.ThrowsAsync<UniqueConstraintException>(() => store.Insert(new UqDeterministicUser { Id = "u2", Email = "a@x.com" }));
    }

    [Fact]
    public async Task NamedIndex_UsesTheGivenName()
    {
        using var store = CreateStore(o => o.ConfigureDocument<UqKeyDoc>(cfg => cfg.MapUniqueIndex(x => x.Email, name: "customer_email")));

        await store.Insert(new UqKeyDoc { Id = "k1", Email = "a@x.com" });
        var ex = await Assert.ThrowsAsync<UniqueConstraintException>(() => store.Insert(new UqKeyDoc { Id = "k2", Email = "a@x.com" }));

        Assert.Equal("uq_UqKeyDoc_customer_email", ex.IndexName);
    }

    [Fact]
    public void KeyParts_MustBeDistinctPropertyAccesses()
    {
        Assert.Throws<ArgumentException>(() => MappingsOf(o => o.ConfigureDocument<UqKeyDoc>(cfg => cfg.MapUniqueIndex(x => x.Email!.ToLower()))));
        Assert.Throws<ArgumentException>(() => MappingsOf(o => o.ConfigureDocument<UqKeyDoc>(cfg => cfg.MapUniqueIndex(x => new { x.Email, Again = x.Email }))));
    }

    [Fact]
    public void IndexNames_AreCappedAndDeterministic()
    {
        var name = new string('n', 100);
        var first = Assert.Single(MappingsOf(o => o.ConfigureDocument<UqKeyDoc>(cfg => cfg.MapUniqueIndex(x => x.Email, name: name))).ResolveUniqueIndexes(typeof(UqKeyDoc)));
        var second = Assert.Single(MappingsOf(o => o.ConfigureDocument<UqKeyDoc>(cfg => cfg.MapUniqueIndex(x => x.Email, name: name))).ResolveUniqueIndexes(typeof(UqKeyDoc)));

        Assert.True(first.Name.Length <= 51);
        Assert.StartsWith("uq_UqKeyDoc_nnn", first.Name);
        Assert.Equal(first.Name, second.Name);

        // Index names are schema-wide on some engines, so each table gets its own storage name.
        Assert.True(first.GetStorageName("documents").Length <= 60);
        Assert.NotEqual(first.GetStorageName("documents"), first.GetStorageName("archive"));
        Assert.Equal(first.GetStorageName("documents"), second.GetStorageName("documents"));
    }

    [Fact]
    public void RedeclaringAnIndex_ReplacesIt()
    {
        var mappings = MappingsOf(o => o.ConfigureDocument<UqKeyDoc>(cfg =>
        {
            cfg.MapUniqueIndex(x => x.Email);
            cfg.MapUniqueIndex(x => x.Email, filter: x => !x.IsDeleted);
        }));

        var index = Assert.Single(mappings.ResolveUniqueIndexes(typeof(UqKeyDoc)));
        Assert.NotNull(index.Filter);
    }

    static readonly JsonSerializerOptions KeyJson = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        TypeInfoResolver = new DefaultJsonTypeInfoResolver()
    };

    static IReadOnlyList<UniqueIndexEntry> Keys(IReadOnlyList<UniqueIndexMapping> indexes, UqKeyDoc doc, string typeName = "UqKeyDoc")
        => UniqueIndexKeys.Compute(indexes, typeName, doc, JsonSerializer.Serialize(doc, KeyJson), KeyJson);

    [Fact]
    public void Keys_ExcludeNullMissingAndFilteredDocuments()
    {
        var indexes = MappingsOf(o => o.ConfigureDocument<UqKeyDoc>(cfg =>
        {
            cfg.MapUniqueIndex(x => x.Email);
            cfg.MapUniqueIndex(x => new { x.Region, x.Email }, filter: x => !x.IsDeleted);
        })).ResolveUniqueIndexes(typeof(UqKeyDoc));

        Assert.Single(Keys(indexes, new UqKeyDoc { Id = "k", Email = "a@x.com" }));                                  // composite: region null
        Assert.Single(Keys(indexes, new UqKeyDoc { Id = "k", Email = "a@x.com", Region = "eu", IsDeleted = true })); // composite: filtered out
        Assert.Equal(2, Keys(indexes, new UqKeyDoc { Id = "k", Email = "a@x.com", Region = "eu" }).Count);
        Assert.Empty(Keys(indexes, new UqKeyDoc { Id = "k", Region = "eu" }));                                       // both: email null
    }

    [Fact]
    public void Keys_AreExactAndScopedToTheType()
    {
        var indexes = MappingsOf(o => o.ConfigureDocument<UqKeyDoc>(cfg => cfg.MapUniqueIndex(x => x.Email))).ResolveUniqueIndexes(typeof(UqKeyDoc));

        var lower = Assert.Single(Keys(indexes, new UqKeyDoc { Id = "k1", Email = "a@x.com" }));
        var sameValue = Assert.Single(Keys(indexes, new UqKeyDoc { Id = "k2", Email = "a@x.com" }));
        var upper = Assert.Single(Keys(indexes, new UqKeyDoc { Id = "k3", Email = "A@X.COM" }));
        var otherType = Assert.Single(Keys(indexes, new UqKeyDoc { Id = "k4", Email = "a@x.com" }, typeName: "Other"));

        Assert.Equal(lower.Key, sameValue.Key);
        Assert.NotEqual(lower.Key, upper.Key);
        Assert.NotEqual(lower.Key, otherType.Key);
        Assert.Equal(64, lower.Hash.Length);
    }

    [Fact]
    public void Keys_TreatEqualNumbersAsOneValue()
    {
        var indexes = MappingsOf(o => o.ConfigureDocument<UqKeyDoc>(cfg => cfg.MapUniqueIndex(x => x.Amount))).ResolveUniqueIndexes(typeof(UqKeyDoc));

        var scaled = Assert.Single(Keys(indexes, new UqKeyDoc { Id = "k1", Amount = 1.50m }));
        var plain = Assert.Single(Keys(indexes, new UqKeyDoc { Id = "k2", Amount = 1.5m }));
        var other = Assert.Single(Keys(indexes, new UqKeyDoc { Id = "k3", Amount = 15m }));

        Assert.Equal(scaled.Key, plain.Key);
        Assert.NotEqual(scaled.Key, other.Key);
    }

    [Fact]
    public void Diff_ClaimsNewEntriesAndReleasesDroppedOnes()
    {
        var indexes = MappingsOf(o => o.ConfigureDocument<UqKeyDoc>(cfg => cfg.MapUniqueIndex(x => x.Email))).ResolveUniqueIndexes(typeof(UqKeyDoc));
        var before = Keys(indexes, new UqKeyDoc { Id = "k", Email = "a@x.com" });
        var after = Keys(indexes, new UqKeyDoc { Id = "k", Email = "b@x.com" });

        var (added, removed) = UniqueIndexKeys.Diff(before, after);
        Assert.Equal(after[0].Key, Assert.Single(added).Key);
        Assert.Equal(before[0].Key, Assert.Single(removed).Key);

        var (noneAdded, noneRemoved) = UniqueIndexKeys.Diff(before, Keys(indexes, new UqKeyDoc { Id = "k", Email = "a@x.com" }));
        Assert.Empty(noneAdded);
        Assert.Empty(noneRemoved);
    }

    [Theory]
    [InlineData("SQLite Error 19: 'UNIQUE constraint failed: index 'uq_User_Email''.", true)]
    [InlineData("23505: duplicate key value violates unique constraint \"uq_user_email\"", true)]
    [InlineData("Duplicate entry 'x' for key 'documents.uq_User_Email'", true)]
    [InlineData("ORA-00001: unique constraint (SYSTEM.uq_User_Email) violated", true)]
    [InlineData("UNIQUE constraint failed: index 'uq_User_Email__Region'", false)]
    [InlineData("UNIQUE constraint failed: documents.Id, documents.TypeName", false)]
    public void MentionsIdentifier_MatchesWholeIdentifiersOnly(string message, bool expected)
        => Assert.Equal(expected, IDatabaseProvider.MentionsIdentifier(message, "uq_User_Email"));
}
