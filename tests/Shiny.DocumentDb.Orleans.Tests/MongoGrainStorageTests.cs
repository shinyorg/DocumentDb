using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Shiny.DocumentDb.MongoDb;
using Xunit;

namespace Shiny.DocumentDb.Orleans.Tests;

[Collection("Mongo")]
public class MongoGrainStorageTests(MongoFixture fixture) : GrainStorageTestsBase
{
    static readonly IServiceProvider EmptyServices = new ServiceCollection().BuildServiceProvider();

    protected override DocumentDbGrainStorage CreateStorage()
    {
        var connectionString = fixture.Container.GetConnectionString();
        var collection = "orleans_" + Guid.NewGuid().ToString("N");

        var options = new DocumentDbGrainStorageOptions
        {
            // Exercises the generic IDocumentStore path the MongoDb companion package wires up.
            StoreFactory = _ =>
            {
                var so = new MongoDbDocumentStoreOptions
                {
                    ConnectionString = connectionString,
                    DatabaseName = "orleans_test"
                };
                so.MapTypeToCollection<GrainStateRecord>(collection);
                so.MapVersionProperty<GrainStateRecord>(x => x.Version);
                return new MongoDbDocumentStore(so);
            }
        };
        return new DocumentDbGrainStorage("mongo", options, EmptyServices, NullLogger<DocumentDbGrainStorage>.Instance);
    }
}
