using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using Shiny.DocumentDb;
using Shiny.DocumentDb.Sqlite;
using ShinyDocDbMyAdmin.Models;
using ShinyDocDbMyAdmin.Services;

namespace ShinyDocDbMyAdmin.Tests;

/// <summary>
/// Soft delete as the admin tool can see it: not at all until it is declared, and then completely.
/// </summary>
/// <remarks>
/// <para>
/// The two halves are deliberately tested against each other. <c>AddSoftDelete</c> writes no DDL, so the
/// first half asserts that a store using it looks <b>identical</b> in the catalog to one that is not — no
/// new table, no feature in the verdict — and that the schema sample can offer nothing better than a
/// candidate. The second half asserts what a declaration buys: the delete button flags instead of deleting,
/// Browse partitions, and restore writes the value the library's own <c>RestoreValue</c> would.
/// </para>
/// <para>
/// The undeclared case is the one worth being loud about. Before this, the admin's delete was a real
/// <c>DELETE</c> against a type the application would only ever have flagged — an invariant broken with no
/// warning. It now refuses unless the caller says "permanently", and the test for that is the point of the
/// change.
/// </para>
/// </remarks>
public sealed class SoftDeleteAdminTests : IAsyncLifetime
{
    sealed class Customer
    {
        public string Id { get; set; } = "";
        public string Name { get; set; } = "";
        public bool IsDeleted { get; set; }
        public bool IsActive { get; set; }
        public string? DeletedBy { get; set; }
    }

    sealed class Invoice
    {
        public string Id { get; set; } = "";
        public DateTimeOffset? DeletedAt { get; set; }
    }

    const string Table = "documents";

    string directory = "";
    string databasePath = "";
    string profileId = "";
    DocumentAdminService admin = null!;
    ConnectionManager connections = null!;
    ProfileStore profiles = null!;

    /// <summary>What has been declared so far, as the host would have configured it.</summary>
    readonly Dictionary<string, (string Path, SoftDeleteFlagKind Kind)> declared = new(StringComparer.Ordinal);

    public ValueTask InitializeAsync()
    {
        this.directory = Path.Combine(Path.GetTempPath(), $"docdbsoft_{Guid.NewGuid():N}");
        Directory.CreateDirectory(this.directory);
        this.databasePath = Path.Combine(this.directory, "store.db");
        this.Build();
        return ValueTask.CompletedTask;
    }

    void Build()
    {
        this.connections?.Dispose();

        var settings = new Dictionary<string, string?>
        {
            ["ShinyDocDbMyAdmin:DataDirectory"] = Path.Combine(this.directory, "admin"),
            ["ShinyDocDbMyAdmin:SecretKey"] = "test-key",
            ["ConnectionStrings:store"] = $"Data Source={this.databasePath}",
            ["Shiny:DocumentDb:store:Provider"] = "Sqlite"
        };

        // The long configuration form, so both fields are exercised. A provided connection is never written
        // to the profile store, so this is the only place its declarations can live.
        foreach (var (type, flag) in this.declared)
        {
            settings[$"Shiny:DocumentDb:store:{ProvidedConnections.SoftDeleteSetting}:{type}:PropertyPath"] = flag.Path;
            settings[$"Shiny:DocumentDb:store:{ProvidedConnections.SoftDeleteSetting}:{type}:FlagKind"] = flag.Kind.ToString();
        }

        var configuration = new ConfigurationBuilder().AddInMemoryCollection(settings).Build();

        var paths = new AppPaths(configuration);
        var protector = new SecretProtector(configuration, paths, NullLogger<SecretProtector>.Instance);
        var provided = new ProvidedConnections(configuration, NullLogger<ProvidedConnections>.Instance);
        this.profiles = new ProfileStore(paths, protector, provided,
            new DemoMode(configuration, NullLogger<DemoMode>.Instance));

        this.profileId = provided.Profiles.Single().Id;
        this.connections = new ConnectionManager(this.profiles, NullLogger<ConnectionManager>.Instance);
        this.admin = new DocumentAdminService(this.connections, NullLogger<DocumentAdminService>.Instance);
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

    CancellationToken Ct => TestContext.Current.CancellationToken;

    // ── Seeding ─────────────────────────────────────────────────────────

    DocumentStore OpenStore(bool softDelete)
    {
        var options = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider($"Data Source={this.databasePath}"),
            TableName = Table
        };

        if (softDelete)
        {
            options.ConfigureDocument<Customer>(cfg => cfg.AddSoftDelete(x => x.IsDeleted));
            options.ConfigureDocument<Invoice>(cfg => cfg.AddSoftDelete(x => x.DeletedAt));
        }

        return new DocumentStore(options);
    }

    /// <summary>Three customers, one of which the application has soft-deleted through its own interceptor.</summary>
    async Task SeedCustomers()
    {
        using var store = this.OpenStore(softDelete: true);
        await store.Insert(new Customer { Id = "c1", Name = "ann", IsActive = true });
        await store.Insert(new Customer { Id = "c2", Name = "bo", IsActive = true });
        await store.Insert(new Customer { Id = "c3", Name = "cy", IsActive = false });

        // Goes through the interceptor: the row survives with IsDeleted = true.
        await store.Remove<Customer>("c3");
    }

    async Task SeedInvoices()
    {
        using var store = this.OpenStore(softDelete: true);
        await store.Insert(new Invoice { Id = "i1" });
        await store.Insert(new Invoice { Id = "i2" });
        await store.Remove<Invoice>("i2");
    }

    void Declare(string typeName, string path, SoftDeleteFlagKind kind)
    {
        this.declared[typeName] = (path, kind);
        this.Build();
    }

    Task<DocumentPage> Browse(string typeName, DeletedFilter filter)
        => this.admin.Browse(this.profileId, Table, typeName, new BrowseQuery { Deleted = filter }, this.Ct);

    // ── Undeclared: nothing is known, and nothing pretends to be ────────

    [Fact]
    public async Task SoftDeleteLeavesNothingInTheCatalog_SoTheVerdictNeverClaimsIt()
    {
        await this.SeedCustomers();

        var identity = await this.admin.GetIdentity(this.profileId, refresh: true, ct: this.Ct);
        var tables = await this.admin.ListTables(this.profileId, refresh: true, this.Ct);

        // Exactly what a store with no soft delete would look like: one documents table, no sidecar of any
        // kind, and a feature list that does not mention it.
        Assert.True(identity.Participates);
        Assert.DoesNotContain("soft delete", identity.Features);
        Assert.Single(tables, t => t.IsOwned);
    }

    [Fact]
    public async Task AFlaggedDocumentIsACandidate_AndACandidateChangesNothing()
    {
        await this.SeedCustomers();

        var schema = await this.admin.InferSchema(this.profileId, Table, nameof(Customer), ct: this.Ct);
        var flag = schema.Fields.Single(f => f.Path == "isDeleted");

        Assert.Equal(SoftDeleteFlagKind.Boolean, flag.SoftDeleteCandidate);
        Assert.True(flag.IsSoftDeleteCandidate);

        // And that is all it does. A candidate is a suggestion for the operator - it declares nothing, so
        // every behaviour downstream of a declaration is still off.
        Assert.Null(await this.admin.GetSoftDeleteFlag(this.profileId, nameof(Customer), this.Ct));

        var identity = await this.admin.GetIdentity(this.profileId, refresh: true, ct: this.Ct);
        Assert.DoesNotContain("soft delete", identity.Features);

        // Including the browse partition: with no declaration there is nothing to partition on, so the
        // flagged document is in the page like any other.
        var page = await this.Browse(nameof(Customer), DeletedFilter.Live);
        Assert.Equal(3, page.TotalCount);
    }

    [Fact]
    public async Task ATimestampFlagIsACandidateToo()
    {
        await this.SeedInvoices();

        var schema = await this.admin.InferSchema(this.profileId, Table, nameof(Invoice), ct: this.Ct);
        var flag = schema.Fields.Single(f => f.Path == "deletedAt");

        // Null on the live invoice, a timestamp on the removed one - the second of the two shapes
        // SoftDeleteMapping.Build accepts.
        Assert.Equal(SoftDeleteFlagKind.Timestamp, flag.SoftDeleteCandidate);
    }

    [Fact]
    public async Task AFieldThatFailsEitherHalfOfTheTestIsNotACandidate()
    {
        await this.SeedCustomers();

        var schema = await this.admin.InferSchema(this.profileId, Table, nameof(Customer), ct: this.Ct);

        // Right shape, wrong name: a bool that has nothing to do with deletion.
        Assert.Null(schema.Fields.Single(f => f.Path == "isActive").SoftDeleteCandidate);

        // Right name, wrong shape: strings that are not timestamps and are never null.
        Assert.Null(schema.Fields.SingleOrDefault(f => f.Path == "deletedBy")?.SoftDeleteCandidate);
    }

    [Fact]
    public async Task ADomainFieldCalledIsDeleted_ChangesNoRoleAndNoBehaviour()
    {
        // The worst case for name matching: a store with no soft delete at all whose documents happen to
        // carry an IsDeleted property that means something else entirely.
        using (var store = this.OpenStore(softDelete: false))
        {
            await store.Insert(new Customer { Id = "c1", Name = "ann", IsDeleted = true });
            await store.Insert(new Customer { Id = "c2", Name = "bo" });
        }

        var schema = await this.admin.InferSchema(this.profileId, Table, nameof(Customer), ct: this.Ct);
        Assert.True(schema.Fields.Single(f => f.Path == "isDeleted").IsSoftDeleteCandidate);

        var tables = await this.admin.ListTables(this.profileId, refresh: true, this.Ct);
        var identity = await this.admin.GetIdentity(this.profileId, refresh: true, ct: this.Ct);

        // Nothing moved. The candidate is the entire effect, and its effect is a prompt.
        Assert.Equal(TableRole.Documents, tables.Single(t => t.Name == Table).Role);
        Assert.True(tables.Single(t => t.Name == Table).IsBrowsable);
        Assert.DoesNotContain("soft delete", identity.Features);
        Assert.Equal(2, (await this.Browse(nameof(Customer), DeletedFilter.Live)).TotalCount);

        // And the delete button still deletes, because nothing was declared.
        Assert.Equal(1, await this.admin.DeleteDocuments(this.profileId, Table, nameof(Customer), ["c1"], ct: this.Ct));
        Assert.Null(await this.admin.GetDocument(this.profileId, Table, nameof(Customer), "c1", this.Ct));
    }

    [Fact]
    public async Task AnUndeclaredTypeIsStillHardDeleted()
    {
        await this.SeedCustomers();

        var deleted = await this.admin.DeleteDocuments(this.profileId, Table, nameof(Customer), ["c1"], ct: this.Ct);

        Assert.Equal(1, deleted);
        Assert.Null(await this.admin.GetDocument(this.profileId, Table, nameof(Customer), "c1", this.Ct));
    }

    // ── Declared: the tool can act on it ────────────────────────────────

    [Fact]
    public async Task DeclaringATypePutsSoftDeleteInTheVerdict()
    {
        await this.SeedCustomers();
        this.Declare(nameof(Customer), "isDeleted", SoftDeleteFlagKind.Boolean);

        var identity = await this.admin.GetIdentity(this.profileId, refresh: true, ct: this.Ct);

        Assert.Contains("soft delete", identity.Features);
    }

    [Fact]
    public async Task BrowsePartitionsADeclaredTypeIntoLiveAndDeleted()
    {
        await this.SeedCustomers();
        this.Declare(nameof(Customer), "isDeleted", SoftDeleteFlagKind.Boolean);

        Assert.Equal(2, (await this.Browse(nameof(Customer), DeletedFilter.Live)).TotalCount);
        Assert.Equal(1, (await this.Browse(nameof(Customer), DeletedFilter.Deleted)).TotalCount);
        Assert.Equal(3, (await this.Browse(nameof(Customer), DeletedFilter.All)).TotalCount);

        var deleted = await this.Browse(nameof(Customer), DeletedFilter.Deleted);
        Assert.Equal("c3", deleted.Rows.Single().Id);
    }

    [Fact]
    public async Task ATimestampFlagPartitionsOnWhetherItIsSet()
    {
        await this.SeedInvoices();
        this.Declare(nameof(Invoice), "deletedAt", SoftDeleteFlagKind.Timestamp);

        Assert.Equal("i1", (await this.Browse(nameof(Invoice), DeletedFilter.Live)).Rows.Single().Id);
        Assert.Equal("i2", (await this.Browse(nameof(Invoice), DeletedFilter.Deleted)).Rows.Single().Id);
    }

    [Fact]
    public async Task DeletingADeclaredTypeRefusesUnlessItIsMeantPermanently()
    {
        await this.SeedCustomers();
        this.Declare(nameof(Customer), "isDeleted", SoftDeleteFlagKind.Boolean);

        // This is the hazard the declaration exists to close: the application only ever flags a Customer,
        // so a plain DELETE here would break an invariant it relies on.
        var refused = await Assert.ThrowsAsync<InvalidOperationException>(
            () => this.admin.DeleteDocuments(this.profileId, Table, nameof(Customer), ["c1"], ct: this.Ct));

        Assert.Contains("soft delete", refused.Message, StringComparison.OrdinalIgnoreCase);
        Assert.NotNull(await this.admin.GetDocument(this.profileId, Table, nameof(Customer), "c1", this.Ct));

        // Said on purpose, it still works.
        Assert.Equal(1, await this.admin.DeleteDocuments(this.profileId, Table, nameof(Customer), ["c1"], permanent: true, this.Ct));
        Assert.Null(await this.admin.GetDocument(this.profileId, Table, nameof(Customer), "c1", this.Ct));
    }

    [Fact]
    public async Task FlaggingADocumentLeavesTheRowAndSetsTheFlag()
    {
        await this.SeedCustomers();
        this.Declare(nameof(Customer), "isDeleted", SoftDeleteFlagKind.Boolean);

        Assert.Equal(1, await this.admin.SoftDeleteDocuments(this.profileId, Table, nameof(Customer), ["c1"], this.Ct));

        var row = await this.admin.GetDocument(this.profileId, Table, nameof(Customer), "c1", this.Ct);
        Assert.NotNull(row);
        Assert.True(DocumentAdminService.IsFlagged(row, (await this.admin.GetSoftDeleteFlag(this.profileId, nameof(Customer), this.Ct))!));

        Assert.Equal(1, (await this.Browse(nameof(Customer), DeletedFilter.Live)).TotalCount);
        Assert.Equal(2, (await this.Browse(nameof(Customer), DeletedFilter.Deleted)).TotalCount);
    }

    [Fact]
    public async Task TheApplicationAgreesTheFlaggedDocumentIsDeleted()
    {
        await this.SeedCustomers();
        this.Declare(nameof(Customer), "isDeleted", SoftDeleteFlagKind.Boolean);
        await this.admin.SoftDeleteDocuments(this.profileId, Table, nameof(Customer), ["c1"], this.Ct);

        // The claim worth proving: what the tool wrote is what the library's own query filter reads. A flag
        // this tool set in a shape the interceptor would not have produced would look right here and be
        // invisible to the application.
        using var store = this.OpenStore(softDelete: true);
        var live = await store.Query<Customer>().ToList(this.Ct);

        Assert.Equal(["c2"], live.Select(c => c.Id));
    }

    [Fact]
    public async Task RestoringWritesTheValueTheLibraryWouldRestoreTo()
    {
        await this.SeedCustomers();
        await this.SeedInvoices();
        this.Declare(nameof(Customer), "isDeleted", SoftDeleteFlagKind.Boolean);
        this.Declare(nameof(Invoice), "deletedAt", SoftDeleteFlagKind.Timestamp);

        Assert.Equal(1, await this.admin.RestoreDocuments(this.profileId, Table, nameof(Customer), ["c3"], this.Ct));
        Assert.Equal(1, await this.admin.RestoreDocuments(this.profileId, Table, nameof(Invoice), ["i2"], this.Ct));

        // false for a boolean, null for a timestamp - SoftDeleteMapping.RestoreValue, both of them.
        var customer = await this.admin.GetDocument(this.profileId, Table, nameof(Customer), "c3", this.Ct);
        var invoice = await this.admin.GetDocument(this.profileId, Table, nameof(Invoice), "i2", this.Ct);

        Assert.Equal(System.Text.Json.JsonValueKind.False, customer!.Read("isDeleted")!.GetValueKind());
        Assert.Null(invoice!.Read("deletedAt"));

        Assert.Equal(3, (await this.Browse(nameof(Customer), DeletedFilter.Live)).TotalCount);
        Assert.Equal(0, (await this.Browse(nameof(Customer), DeletedFilter.Deleted)).TotalCount);
    }

    [Fact]
    public async Task FlaggingAnUndeclaredTypeIsRefusedRatherThanGuessedAt()
    {
        await this.SeedCustomers();

        var refused = await Assert.ThrowsAsync<InvalidOperationException>(
            () => this.admin.SoftDeleteDocuments(this.profileId, Table, nameof(Customer), ["c1"], this.Ct));

        Assert.Contains("no declared soft-delete flag", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ClearingADeclaredTypeAlsoHasToBeMeantPermanently()
    {
        await this.SeedCustomers();
        this.Declare(nameof(Customer), "isDeleted", SoftDeleteFlagKind.Boolean);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => this.admin.ClearType(this.profileId, Table, nameof(Customer), ct: this.Ct));

        Assert.Equal(3, await this.admin.ClearType(this.profileId, Table, nameof(Customer), permanent: true, this.Ct));
    }

    [Fact]
    public async Task ASavedProfilesDeclarationTakesEffectWithoutReopeningTheConnection()
    {
        await this.SeedCustomers();

        // A saved connection declares through the profile store rather than through configuration - what the
        // Structure tab's "Declare" button does. The connection handle is cached per profile and the
        // declaration is not a provider input, so it has to be part of what invalidates that cache: without
        // it the next browse would be answered against yesterday's list.
        var saved = new ConnectionProfile { Name = "saved", Provider = Providers.ProviderKind.Sqlite };
        await this.profiles.Save(saved, $"Data Source={this.databasePath}", null, this.Ct);

        var page = await this.admin.Browse(
            saved.Id, Table, nameof(Customer), new BrowseQuery { Deleted = DeletedFilter.Live }, this.Ct);
        Assert.Equal(3, page.TotalCount);

        await this.profiles.SaveSoftDeleteFlags(
            saved.Id,
            [new SoftDeleteFlag { TypeName = nameof(Customer), PropertyPath = "isDeleted" }],
            this.Ct);

        Assert.NotNull(await this.admin.GetSoftDeleteFlag(saved.Id, nameof(Customer), this.Ct));

        page = await this.admin.Browse(
            saved.Id, Table, nameof(Customer), new BrowseQuery { Deleted = DeletedFilter.Live }, this.Ct);
        Assert.Equal(2, page.TotalCount);
    }

    [Fact]
    public async Task TheCompactConfigurationFormDeclaresABooleanFlag()
    {
        await this.SeedCustomers();

        // Shiny:DocumentDb:store:SoftDelete:Customer = isDeleted - the shape an AppHost is most likely to
        // write, and the one a boolean flag needs nothing more than.
        this.connections.Dispose();
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["ShinyDocDbMyAdmin:DataDirectory"] = Path.Combine(this.directory, "admin"),
                ["ShinyDocDbMyAdmin:SecretKey"] = "test-key",
                ["ConnectionStrings:store"] = $"Data Source={this.databasePath}",
                ["Shiny:DocumentDb:store:Provider"] = "Sqlite",
                [$"Shiny:DocumentDb:store:{ProvidedConnections.SoftDeleteSetting}:{nameof(Customer)}"] = "isDeleted"
            })
            .Build();

        var provided = new ProvidedConnections(configuration, NullLogger<ProvidedConnections>.Instance);
        var flag = provided.Profiles.Single().SoftDeleteFlags.Single();

        Assert.Equal(nameof(Customer), flag.TypeName);
        Assert.Equal("isDeleted", flag.PropertyPath);
        Assert.Equal(SoftDeleteFlagKind.Boolean, flag.FlagKind);
    }
}
