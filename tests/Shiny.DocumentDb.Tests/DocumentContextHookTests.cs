using Microsoft.Extensions.DependencyInjection;
using Shiny.DocumentDb.Sqlite;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// The generated <c>OnConfiguring</c> hook — a context declaring its document model next to its
/// <c>[Document]</c> list rather than inside <c>AddDocumentStore</c>. The generated <c>ConfigureModel</c>
/// calls it last, so the hook wins over the attribute-derived mapping and over nothing else.
/// </summary>
public class DocumentContextHookTests
{
    static ServiceProvider Build()
    {
        var services = new ServiceCollection();
        services.AddHookedDocumentContext(o =>
        {
            o.DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:");
            o.UseReflectionFallback = false;
        });
        return services.BuildServiceProvider();
    }

    [Fact]
    public void OnConfiguring_Runs_AndItsTableWins()
    {
        var options = new DocumentStoreOptions { DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:") };
        HookedDocumentContext.ConfigureModel(options);

        Assert.Equal(HookedDocumentContext.UserTable, options.ResolveTableName(nameof(User)));
    }

    [Fact]
    public void OnConfiguring_QueryFilter_IsRegistered()
    {
        var options = new DocumentStoreOptions { DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:") };
        HookedDocumentContext.ConfigureModel(options);

        Assert.Contains(options.ResolveQueryFilters(typeof(User)), f => f.Name == "adults");
    }

    [Fact]
    public async Task OnConfiguring_QueryFilter_AppliesToReads()
    {
        using var provider = Build();
        using var scope = provider.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<HookedDocumentContext>();

        await context.Users.Insert(new User { Id = "u1", Name = "Adult", Age = 30, Email = "a@x.com" });
        await context.Users.Insert(new User { Id = "u2", Name = "Minor", Age = 12, Email = "m@x.com" });

        var visible = await context.Users.Query().ToList();
        Assert.Equal(["u1"], visible.Select(u => u.Id));
    }
}
