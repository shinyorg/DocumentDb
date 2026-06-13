using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans;
using Orleans.GrainDirectory;
using Orleans.Runtime.Hosting;
using Shiny.DocumentDb.Orleans;

namespace Orleans.Hosting;

public static class OrleansSystemStoreSiloBuilderExtensions
{
    // ---- Reminders (IReminderTable) ----

    /// <summary>Adds a DocumentDb-backed Orleans reminder table (calls <c>AddReminders()</c> for you).</summary>
    public static ISiloBuilder AddDocumentDbReminders(
        this ISiloBuilder builder,
        Action<DocumentDbReminderOptions> configure
    ) => builder
        .AddReminders()
        .ConfigureServices(services =>
        {
            services.AddOptions<DocumentDbReminderOptions>().Configure(configure);
            services.AddSingleton<IReminderTable, DocumentDbReminderTable>();
        });

    // ---- Clustering / Membership (IMembershipTable) ----

    /// <summary>
    /// Adds DocumentDb-backed Orleans cluster membership. Requires a backend with multi-document
    /// transactions (relational, or MongoDB on a replica set).
    /// </summary>
    public static ISiloBuilder AddDocumentDbClustering(
        this ISiloBuilder builder,
        Action<DocumentDbMembershipOptions> configure
    ) => builder.ConfigureServices(services =>
    {
        services.AddOptions<DocumentDbMembershipOptions>().Configure(configure);
        services.AddSingleton<IMembershipTable, DocumentDbMembershipTable>();
    });

    // ---- Grain Directory (IGrainDirectory) ----

    /// <summary>Adds a named DocumentDb-backed Orleans grain directory.</summary>
    public static ISiloBuilder AddDocumentDbGrainDirectory(
        this ISiloBuilder builder,
        string name,
        Action<DocumentDbGrainDirectoryOptions> configure
    ) => builder
        .AddGrainDirectory(name, (sp, key) => DocumentDbGrainDirectory.Create(sp, key))
        .ConfigureServices(services =>
            services.AddOptions<DocumentDbGrainDirectoryOptions>(name).Configure(configure));
}
