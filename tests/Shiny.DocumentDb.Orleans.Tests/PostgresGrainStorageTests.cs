using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Shiny.DocumentDb.PostgreSql;
using Xunit;

namespace Shiny.DocumentDb.Orleans.Tests;

[Collection("Postgres")]
public class PostgresGrainStorageTests(PostgresFixture fixture) : GrainStorageTestsBase
{
    static readonly IServiceProvider EmptyServices = new ServiceCollection().BuildServiceProvider();

    protected override DocumentDbGrainStorage CreateStorage()
    {
        var options = new DocumentDbGrainStorageOptions
        {
            DatabaseProvider = new PostgreSqlDatabaseProvider(fixture.Container.GetConnectionString()),
            TableName = "orleans_" + Guid.NewGuid().ToString("N")
        };
        return new DocumentDbGrainStorage("pg", options, EmptyServices, NullLogger<DocumentDbGrainStorage>.Instance);
    }
}
