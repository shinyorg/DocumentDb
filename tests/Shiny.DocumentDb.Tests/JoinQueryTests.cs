using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// Cross-type joins, asserted identically on every provider that runs them — the relational family and MongoDB.
/// </summary>
public abstract class JoinQueryTestsBase(IDocumentStoreFixture fixture)
{
    static readonly IDocumentEncryptor Encryptor = new AesGcmDocumentEncryptor("join", AesGcmDocumentEncryptor.GenerateKey());

    protected readonly IDocumentStoreFixture Fixture = fixture;

    /// <summary>What the provider's rendered join contains: <c>JOIN</c> in SQL, <c>$lookup</c> on MongoDB.</summary>
    protected virtual string QueryStringToken => "JOIN";

    async Task<IDocumentStore> Seeded(Action<IDocumentStoreOptions>? configure = null)
    {
        var table = $"t{Guid.NewGuid():N}";
        var store = configure == null ? this.Fixture.CreateStore(table) : this.Fixture.CreateStore(table, configure);

        await store.Insert(new JoinCustomer { Id = "c1", Name = "Acme", Region = "eu", CreditLimit = 500 });
        await store.Insert(new JoinCustomer { Id = "c2", Name = "Globex", Region = "us", CreditLimit = 100 });
        await store.Insert(new JoinCustomer { Id = "c3", Name = "Initech", Region = "eu", CreditLimit = 50, IsDeleted = true });

        await store.Insert(new JoinOrder { Id = "o1", CustomerId = "c1", Status = "open", Total = 120 });
        await store.Insert(new JoinOrder { Id = "o2", CustomerId = "c1", Status = "shipped", Total = 40 });
        await store.Insert(new JoinOrder { Id = "o3", CustomerId = "c2", Status = "open", Total = 300 });
        await store.Insert(new JoinOrder { Id = "o4", CustomerId = "c3", Status = "open", Total = 70 });
        await store.Insert(new JoinOrder { Id = "o5", CustomerId = "nobody", Status = "open", Total = 10 });
        return store;
    }

    static void Dispose(IDocumentStore store) => (store as IDisposable)?.Dispose();

    // ── Join kinds ──────────────────────────────────────────────────────

    [Fact]
    public async Task InnerJoin_ReturnsMatchedPairsOnly()
    {
        var store = await this.Seeded();
        try
        {
            var rows = await store.Query<JoinOrder>()
                .Join<JoinCustomer>((o, c) => o.CustomerId == c.Id)
                .OrderBy((o, c) => o.Id)
                .Select((o, c) => new JoinRow { OrderId = o.Id, Customer = c.Name, Total = o.Total })
                .ToList();

            Assert.Equal(new[] { "o1", "o2", "o3", "o4" }, rows.Select(r => r.OrderId));
            Assert.Equal(new[] { "Acme", "Acme", "Globex", "Initech" }, rows.Select(r => r.Customer));
            Assert.Equal(120, rows[0].Total);
        }
        finally { Dispose(store); }
    }

    [Fact]
    public async Task LeftJoin_KeepsUnmatchedLeftDocuments()
    {
        var store = await this.Seeded();
        try
        {
            var rows = await store.Query<JoinOrder>()
                .Join<JoinCustomer>((o, c) => o.CustomerId == c.Id, JoinKind.Left)
                .OrderBy((o, c) => o.Id)
                .Select((o, c) => new { o.Id, Customer = c == null ? null : c.Name })
                .ToList();

            Assert.Equal(new[] { "o1", "o2", "o3", "o4", "o5" }, rows.Select(r => r.Id));
            Assert.Null(rows[4].Customer);
            Assert.Equal("Initech", rows[3].Customer);
        }
        finally { Dispose(store); }
    }

    [Fact]
    public async Task LeftJoin_CanFilterForTheUnmatchedRows()
    {
        var store = await this.Seeded();
        try
        {
            var orphans = await store.Query<JoinOrder>()
                .Join<JoinCustomer>((o, c) => o.CustomerId == c.Id, JoinKind.Left)
                .Where((o, c) => c == null)
                .Select((o, c) => o.Id)
                .ToList();

            Assert.Equal("o5", Assert.Single(orphans));
        }
        finally { Dispose(store); }
    }

    [Fact]
    public async Task LeftJoin_ReadingTheMissingRightDocument_ExplainsItself()
    {
        var store = await this.Seeded();
        try
        {
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => store.Query<JoinOrder>()
                .Join<JoinCustomer>((o, c) => o.CustomerId == c.Id, JoinKind.Left)
                .Select((o, c) => c.Name)
                .ToList());

            Assert.Contains("null", ex.Message);
        }
        finally { Dispose(store); }
    }

    // ── Filters, ordering, paging, terminals ────────────────────────────

    [Fact]
    public async Task Where_FiltersOnEitherSide()
    {
        var store = await this.Seeded();
        try
        {
            var ids = await store.Query<JoinOrder>()
                .Join<JoinCustomer>((o, c) => o.CustomerId == c.Id)
                .Where((o, c) => c.Region == "eu" && o.Total > 50)
                .OrderBy((o, c) => o.Id)
                .Select((o, c) => o.Id)
                .ToList();

            Assert.Equal(new[] { "o1", "o4" }, ids);
        }
        finally { Dispose(store); }
    }

    [Fact]
    public async Task Where_ComparesTheTwoDocuments()
    {
        var store = await this.Seeded();
        try
        {
            var overLimit = await store.Query<JoinOrder>()
                .Join<JoinCustomer>((o, c) => o.CustomerId == c.Id)
                .Where((o, c) => o.Total > c.CreditLimit)
                .OrderBy((o, c) => o.Id)
                .Select((o, c) => o.Id)
                .ToList();

            Assert.Equal(new[] { "o3", "o4" }, overLimit);
        }
        finally { Dispose(store); }
    }

    [Fact]
    public async Task SourceQueryWhere_FiltersTheLeftSide()
    {
        var store = await this.Seeded();
        try
        {
            var ids = await store.Query<JoinOrder>()
                .Where(x => x.Status == "open")
                .Join<JoinCustomer>((o, c) => o.CustomerId == c.Id)
                .OrderBy((o, c) => o.Id)
                .Select((o, c) => o.Id)
                .ToList();

            Assert.Equal(new[] { "o1", "o3", "o4" }, ids);
        }
        finally { Dispose(store); }
    }

    [Fact]
    public async Task OrderBy_EitherSide_WithPaging()
    {
        var store = await this.Seeded();
        try
        {
            var page = await store.Query<JoinOrder>()
                .Join<JoinCustomer>((o, c) => o.CustomerId == c.Id)
                .OrderBy((o, c) => c.Name)
                .OrderByDescending((o, c) => o.Total)
                .Paginate(1, 2)
                .Select((o, c) => o.Id)
                .ToList();

            Assert.Equal(new[] { "o2", "o3" }, page);
        }
        finally { Dispose(store); }
    }

    [Fact]
    public async Task CountAnyFirst()
    {
        var store = await this.Seeded();
        try
        {
            var join = store.Query<JoinOrder>().Join<JoinCustomer>((o, c) => o.CustomerId == c.Id);

            Assert.Equal(4, await join.Select((o, c) => o.Id).Count());
            Assert.True(await join.Select((o, c) => o.Id).Any());
            Assert.False(await join.Where((o, c) => c.Region == "nowhere").Select((o, c) => o.Id).Any());
            Assert.Equal("o1", await join.OrderBy((o, c) => o.Id).Select((o, c) => o.Id).First());
            Assert.Null(await join.Where((o, c) => c.Region == "nowhere").Select((o, c) => o.Id).FirstOrDefault());
        }
        finally { Dispose(store); }
    }

    [Fact]
    public async Task Builder_IsImmutable()
    {
        var store = await this.Seeded();
        try
        {
            var join = store.Query<JoinOrder>().Join<JoinCustomer>((o, c) => o.CustomerId == c.Id);
            var eu = join.Where((o, c) => c.Region == "eu");

            Assert.Equal(4, await join.Select((o, c) => o.Id).Count());
            Assert.Equal(3, await eu.Select((o, c) => o.Id).Count());
        }
        finally { Dispose(store); }
    }

    [Fact]
    public async Task ToAsyncEnumerable_StreamsThePairs()
    {
        var store = await this.Seeded();
        try
        {
            var ids = new List<string>();
            await foreach (var id in store.Query<JoinOrder>()
                               .Join<JoinCustomer>((o, c) => o.CustomerId == c.Id)
                               .OrderBy((o, c) => o.Id)
                               .Select((o, c) => o.Id)
                               .ToAsyncEnumerable())
            {
                ids.Add(id);
            }

            Assert.Equal(new[] { "o1", "o2", "o3", "o4" }, ids);
        }
        finally { Dispose(store); }
    }

    [Fact]
    public async Task ToQueryString_RendersTheJoin()
    {
        var store = await this.Seeded();
        try
        {
            var query = store.Query<JoinOrder>()
                .Join<JoinCustomer>((o, c) => o.CustomerId == c.Id)
                .Select((o, c) => o.Id)
                .ToQueryString();

            Assert.Contains(this.QueryStringToken, query.Sql);
        }
        finally { Dispose(store); }
    }

    [Fact]
    public async Task OrderingBeforeTheJoin_IsRefused()
    {
        var store = await this.Seeded();
        try
        {
            Assert.Throws<InvalidOperationException>(() =>
                store.Query<JoinOrder>().OrderBy(x => x.Id).Join<JoinCustomer>((o, c) => o.CustomerId == c.Id));
        }
        finally { Dispose(store); }
    }

    // ── Query filters, tables, tenancy ──────────────────────────────────

    [Fact]
    public async Task RightSideQueryFilter_IsPartOfTheJoinCondition()
    {
        var store = await this.Seeded(o => o.ConfigureDocument<JoinCustomer>(cfg => cfg.AddQueryFilter("active", c => !c.IsDeleted)));
        try
        {
            var left = await store.Query<JoinOrder>()
                .Join<JoinCustomer>((o, c) => o.CustomerId == c.Id, JoinKind.Left)
                .OrderBy((o, c) => o.Id)
                .Select((o, c) => new { o.Id, Customer = c == null ? null : c.Name })
                .ToList();

            // The filtered-out customer makes o4 unmatched; it does not take the order away.
            Assert.Equal(5, left.Count);
            Assert.Null(left[3].Customer);

            var inner = store.Query<JoinOrder>().Join<JoinCustomer>((o, c) => o.CustomerId == c.Id);
            Assert.Equal(3, await inner.Select((o, c) => o.Id).Count());
            Assert.Equal(4, await inner.IgnoreQueryFilters("active").Select((o, c) => o.Id).Count());
        }
        finally { Dispose(store); }
    }

    [Fact]
    public async Task LeftSideQueryFilter_AppliesAndCanBeIgnored()
    {
        var store = await this.Seeded(o => o.ConfigureDocument<JoinOrder>(cfg => cfg.AddQueryFilter("open", x => x.Status == "open")));
        try
        {
            Assert.Equal(3, await store.Query<JoinOrder>()
                .Join<JoinCustomer>((o, c) => o.CustomerId == c.Id)
                .Select((o, c) => o.Id)
                .Count());

            Assert.Equal(4, await store.Query<JoinOrder>()
                .IgnoreQueryFilters()
                .Join<JoinCustomer>((o, c) => o.CustomerId == c.Id)
                .Select((o, c) => o.Id)
                .Count());
        }
        finally { Dispose(store); }
    }

    [Fact]
    public async Task SidesInDifferentTables()
    {
        var customers = $"c{Guid.NewGuid():N}";
        var store = await this.Seeded(o => o.ConfigureDocument<JoinCustomer>(cfg => cfg.Table = customers));
        try
        {
            var rows = await store.Query<JoinOrder>()
                .Join<JoinCustomer>((o, c) => o.CustomerId == c.Id)
                .OrderBy((o, c) => o.Id)
                .Select((o, c) => c.Name)
                .ToList();

            Assert.Equal(new[] { "Acme", "Acme", "Globex", "Initech" }, rows);
        }
        finally { Dispose(store); }
    }

    [Fact]
    public async Task SharedTableTenancy_ScopesBothSides()
    {
        if (this.Fixture is not ITenantDocumentStoreFixture tenants)
            return;

        var tenant = "a";
        var store = tenants.CreateStoreWithTenant($"t{Guid.NewGuid():N}", () => tenant);
        try
        {
            await store.Insert(new JoinCustomer { Id = "c1", Name = "Acme" });
            await store.Insert(new JoinOrder { Id = "o1", CustomerId = "c1" });

            // Tenant b's order points at tenant a's customer; the join must not reach across.
            tenant = "b";
            await store.Insert(new JoinOrder { Id = "o2", CustomerId = "c1" });

            Assert.Equal(0, await store.Query<JoinOrder>().Join<JoinCustomer>((o, c) => o.CustomerId == c.Id).Select((o, c) => o.Id).Count());

            tenant = "a";
            Assert.Equal(1, await store.Query<JoinOrder>().Join<JoinCustomer>((o, c) => o.CustomerId == c.Id).Select((o, c) => o.Id).Count());
        }
        finally { Dispose(store); }
    }

    // ── Encryption ──────────────────────────────────────────────────────

    [Fact]
    public async Task EncryptedProperty_FiltersOnItsSide_ButCannotBeAJoinKey()
    {
        var store = this.Fixture.CreateStore($"t{Guid.NewGuid():N}", o =>
        {
            // Encryption installs a JsonTypeInfo modifier, so the options need a resolver to attach it to.
            o.SerializerOptions ??= new JsonSerializerOptions(JsonSerializerDefaults.Web) { TypeInfoResolver = new DefaultJsonTypeInfoResolver() };
            o.UseEncryptor(Encryptor);
            o.ConfigureDocument<JoinSecretCustomer>(cfg => cfg.MapProperty(x => x.Code, p => p.Encrypt(EncryptionMode.Deterministic)));
        });
        try
        {
            await store.Insert(new JoinSecretCustomer { Id = "s1", Name = "Acme", Code = "gold" });
            await store.Insert(new JoinSecretCustomer { Id = "s2", Name = "Globex", Code = "silver" });
            await store.Insert(new JoinOrder { Id = "o1", CustomerId = "s1" });
            await store.Insert(new JoinOrder { Id = "o2", CustomerId = "s2" });

            // A constant comparison on one side is rewritten into ciphertext, and the projection reads plaintext.
            var gold = await store.Query<JoinOrder>()
                .Join<JoinSecretCustomer>((o, c) => o.CustomerId == c.Id)
                .Where((o, c) => c.Code == "gold")
                .Select((o, c) => new { o.Id, c.Code })
                .ToList();
            Assert.Equal("o1", Assert.Single(gold).Id);
            Assert.Equal("gold", gold[0].Code);

            var ex = await Assert.ThrowsAsync<NotSupportedException>(() => store.Query<JoinOrder>()
                .Join<JoinSecretCustomer>((o, c) => o.CustomerId == c.Code)
                .Select((o, c) => o.Id)
                .ToList());
            Assert.Contains("JoinSecretCustomer.Code", ex.Message);
        }
        finally { Dispose(store); }
    }

    // ── String syntax ───────────────────────────────────────────────────

    [Fact]
    public async Task StringJoin_WhereOrderByProject()
    {
        var store = await this.Seeded();
        try
        {
            var rows = await store.Query<JoinOrder>()
                .Join<JoinCustomer>("o", "c", "o.customerId = c.id")
                .Where("c.region = 'eu' and o.total > 50")
                .OrderBy("o.id")
                .Project("o.id as orderId, c.name as customer, o.total")
                .ToList();

            Assert.Equal(2, rows.Count);
            Assert.Equal("o1", rows[0]["orderId"]!.GetValue<string>());
            Assert.Equal("Acme", rows[0]["customer"]!.GetValue<string>());
            Assert.Equal(120, rows[0]["total"]!.GetValue<int>());
            Assert.Equal("o4", rows[1]["orderId"]!.GetValue<string>());
        }
        finally { Dispose(store); }
    }

    [Fact]
    public async Task StringJoin_ComparesTheTwoDocuments_AndOrdersDescending()
    {
        var store = await this.Seeded();
        try
        {
            var rows = await store.Query<JoinOrder>()
                .Join<JoinCustomer>("o", "c", "o.customerId = c.id")
                .Where("o.total > c.creditLimit")
                .OrderByDescending("o.total")
                .Project("o.id")
                .ToList();

            Assert.Equal(new[] { "o3", "o4" }, rows.Select(r => r["id"]!.GetValue<string>()));
        }
        finally { Dispose(store); }
    }

    [Fact]
    public async Task StringOverloads_OnALinqJoin_UseTheConditionsParameterNames()
    {
        var store = await this.Seeded();
        try
        {
            var region = "us";
            var ids = await store.Query<JoinOrder>()
                .Join<JoinCustomer>((order, customer) => order.CustomerId == customer.Id)
                .Where($"customer.region = {region}")
                .Select((o, c) => o.Id)
                .ToList();

            Assert.Equal("o3", Assert.Single(ids));
        }
        finally { Dispose(store); }
    }

    [Fact]
    public async Task StringJoin_LeftJoin_ProjectsNullForTheMissingSide()
    {
        var store = await this.Seeded();
        try
        {
            var rows = await store.Query<JoinOrder>()
                .Join<JoinCustomer>("o", "c", "o.customerId = c.id", JoinKind.Left)
                .Where("o.id = 'o5'")
                .Project("o.id, c.name")
                .ToList();

            var row = Assert.Single(rows);
            Assert.Equal("o5", row["id"]!.GetValue<string>());
            Assert.Null(row["name"]);
        }
        finally { Dispose(store); }
    }

    [Fact]
    public async Task StringSyntax_RequiresADeclaredAlias()
    {
        var store = await this.Seeded();
        try
        {
            var join = store.Query<JoinOrder>().Join<JoinCustomer>("o", "c", "o.customerId = c.id");

            Assert.Throws<ArgumentException>(() => join.Where("region = 'eu'"));
            var unknown = Assert.Throws<ArgumentException>(() => join.Where("x.region = 'eu'"));
            Assert.Contains("x", unknown.Message);
            Assert.Throws<ArgumentException>(() => join.Project("o.id, c.id"));
            Assert.Throws<ArgumentException>(() => store.Query<JoinOrder>().Join<JoinCustomer>("o", "o", "o.customerId = o.id"));
        }
        finally { Dispose(store); }
    }
}

/// <summary>Stores whose engine has no join primitive refuse a join up front rather than scanning.</summary>
public abstract class JoinNotSupportedTestsBase(IDocumentStoreFixture fixture)
{
    [Fact]
    public void Join_ThrowsNotSupported()
    {
        var store = fixture.CreateStore($"t{Guid.NewGuid():N}");
        try
        {
            Assert.Throws<NotSupportedException>(() => store.Query<JoinOrder>().Join<JoinCustomer>((o, c) => o.CustomerId == c.Id));
            Assert.Throws<NotSupportedException>(() => store.Query<JoinOrder>().Join<JoinCustomer>("o", "c", "o.customerId = c.id"));
        }
        finally
        {
            (store as IDisposable)?.Dispose();
        }
    }
}
