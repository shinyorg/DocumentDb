using Microsoft.Extensions.DependencyInjection;
using Shiny.DocumentDb.Sqlite;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

// Covers the DI shapes the generator emits for a DocumentContext: the scoped Add<Context>, the singleton
// Add<Context>Factory (the MAUI / Blazor / desktop story), and the multi-context isolation that keying the
// store per context type buys us.
public class MultiContextRegistrationTests
{
    static void ConfigureSqlite(DocumentStoreOptions o)
    {
        o.DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:");
        o.TableName = $"t{Guid.NewGuid():N}";
        o.UseReflectionFallback = false;
    }

    [Fact]
    public void Plain_AddDocumentStore_twice_collides_on_un_keyed_IDocumentStore()
    {
        // The low-level primitive registers an un-keyed singleton, so a second call shadows the first on a
        // single resolve. This is exactly why the generated Add<Context> keys the store per context type.
        var services = new ServiceCollection();
        services.AddDocumentStore(o => { o.DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:"); o.TableName = "table_a"; });
        services.AddDocumentStore(o => { o.DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:"); o.TableName = "table_b"; });

        using var provider = services.BuildServiceProvider();

        Assert.Equal(2, provider.GetServices<IDocumentStore>().Count());
        Assert.Equal("table_b", provider.GetRequiredService<DocumentStoreOptions>().TableName);   // table_a shadowed
    }

    [Fact]
    public void Two_generated_contexts_bind_to_separate_stores()
    {
        var services = new ServiceCollection();
        services.AddSampleDocumentContext(ConfigureSqlite);
        services.AddSecondDocumentContext(ConfigureSqlite);

        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var a = scope.ServiceProvider.GetRequiredService<SampleDocumentContext>();
        var b = scope.ServiceProvider.GetRequiredService<SecondDocumentContext>();

        Assert.NotSame(a.Store, b.Store);   // each context bound to its own keyed store — no shadowing
    }

    [Fact]
    public async Task Factory_is_resolvable_from_root_and_creates_fresh_contexts()
    {
        // The MAUI shape: factory registered as a singleton, injectable anywhere, no ambient scope needed.
        var services = new ServiceCollection();
        services.AddSampleDocumentContextFactory(ConfigureSqlite);

        await using var provider = services.BuildServiceProvider();
        var factory = provider.GetRequiredService<IDocumentContextFactory<SampleDocumentContext>>();

        var db1 = factory.Create();
        var db2 = factory.Create();

        Assert.NotSame(db1, db2);            // a fresh context per Create()
        Assert.Same(db1.Store, db2.Store);   // over the same shared, thread-safe store

        await db1.Users.Insert(new User { Id = "u1", Name = "MAUI", Age = 1 });
        Assert.Equal("MAUI", (await db2.Users.Get("u1"))!.Name);   // same store → visible across contexts
    }

    [Fact]
    public void Factory_and_scoped_for_same_context_share_one_store()
    {
        // Registering both shapes for the same context must not build two stores (TryAdd keyed singleton).
        var services = new ServiceCollection();
        services.AddSampleDocumentContext(ConfigureSqlite);
        services.AddSampleDocumentContextFactory(ConfigureSqlite);

        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();

        var scoped = scope.ServiceProvider.GetRequiredService<SampleDocumentContext>();
        var fromFactory = provider.GetRequiredService<IDocumentContextFactory<SampleDocumentContext>>().Create();

        Assert.Same(scoped.Store, fromFactory.Store);
    }
}
