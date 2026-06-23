using Microsoft.Extensions.Diagnostics.HealthChecks;
using Shiny.DocumentDb;

namespace Shiny.DocumentDb.Aspire.Client.Internal;

/// <summary>
/// A trivial liveness probe: opens a connection from the configured <see cref="IDatabaseProvider"/>
/// and runs <c>SELECT 1</c>. For SQLite this is an immediate open; for the server providers it
/// confirms the database is reachable.
/// </summary>
internal sealed class DocumentStoreHealthCheck(DocumentProviderKind kind, string connectionString) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var provider = DatabaseProviderFactory.Create(kind, connectionString);
            await using var connection = provider.CreateConnection();
            await connection.OpenAsync(cancellationToken);
            await provider.InitializeConnectionAsync(connection, cancellationToken);

            await using var command = connection.CreateCommand();
            command.CommandText = "SELECT 1";
            await command.ExecuteScalarAsync(cancellationToken);

            return HealthCheckResult.Healthy();
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy("DocumentDb store is not reachable.", ex);
        }
    }
}
