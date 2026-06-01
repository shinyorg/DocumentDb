namespace Shiny.DocumentDb.SqlServer;

/// <summary>
/// Configures how the SQL Server provider delivers native change notifications. Change Tracking
/// supplies the per-row deltas (insert/update/delete + the changed Id); Query Notifications
/// (<c>SqlDependency</c>) can optionally provide low-latency push wake-ups instead of fixed-interval
/// polling.
/// </summary>
public sealed class SqlServerChangeFeedOptions
{
    /// <summary>
    /// How often the Change Tracking table is polled for new versions. When
    /// <see cref="UseQueryNotifications"/> is enabled this acts as a maximum wait between pushes.
    /// Default: 2 seconds.
    /// </summary>
    public TimeSpan PollInterval { get; init; } = TimeSpan.FromSeconds(2);

    /// <summary>
    /// When <c>true</c>, uses <c>SqlDependency</c> (Service Broker query notifications) to wake the
    /// poller as soon as the watched data changes, falling back to <see cref="PollInterval"/> if the
    /// notification can't be armed. Requires Service Broker to be enabled on the database.
    /// Default: <c>false</c> (interval polling only).
    /// </summary>
    public bool UseQueryNotifications { get; init; }
}
