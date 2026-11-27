using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Shiny.DocumentDb.Hosting;

namespace Shiny.DocumentDb;

/// <summary>
/// Document expiration (time to live), built on the public query-filter + interceptor surface the same way
/// <see cref="SoftDelete"/> is — it works identically on every provider. <c>MapExpiration</c> registers:
/// <list type="bullet">
/// <item>a named query filter (<see cref="FilterName"/>) that hides a document once its expiry time has passed,
/// from <c>Get</c>, queries, counts and set-based writes alike; and</item>
/// <item>when a default time to live is given, an interceptor that stamps the expiry on insert and upsert whenever
/// the document leaves it null.</item>
/// </list>
/// Expired documents stay stored until <see cref="DocumentExpirationExtensions.PurgeExpired{T}"/> deletes them —
/// call it yourself or register <see cref="DocumentExpirationServiceCollectionExtensions.AddDocumentExpirationSweeper"/>
/// to run it on a timer.
/// </summary>
public static class DocumentExpiration
{
    /// <summary>The name of the query filter <c>MapExpiration</c> registers; lift it per query with
    /// <see cref="DocumentExpirationExtensions.IncludeExpired{T}"/>.</summary>
    public const string FilterName = "expiration";

    // Per document type, process-wide for the same reason as soft delete: the helpers (PurgeExpired, the sweeper)
    // start from a bare IDocumentStore and need to find the mapping.
    static readonly ConcurrentDictionary<Type, ExpirationMapping> mappings = new();

    internal static void Configure<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(
        IDocumentStoreOptions options,
        LambdaExpression expiresAt,
        TimeSpan? defaultTimeToLive,
        TimeProvider? timeProvider) where T : class
    {
        ArgumentNullException.ThrowIfNull(options);
        if (defaultTimeToLive is { } ttl && ttl <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(defaultTimeToLive), ttl, "The default time to live must be positive.");

        var mapping = ExpirationMapping<T>.Build(expiresAt, defaultTimeToLive, timeProvider ?? TimeProvider.System);
        mappings[typeof(T)] = mapping;

        options.Mappings.AddQueryFilter(FilterName, mapping.NotExpired);
        if (defaultTimeToLive != null)
            options.AddInterceptor(new ExpirationInterceptor<T>(mapping));
    }

    internal static ExpirationMapping<T> Require<T>() where T : class
        => mappings.TryGetValue(typeof(T), out var mapping)
            ? (ExpirationMapping<T>)mapping
            : throw new InvalidOperationException(
                $"'{typeof(T).Name}' is not mapped for expiration. Call options.ConfigureDocument<{typeof(T).Name}>(cfg => cfg.MapExpiration(x => x.ExpiresAt)) when configuring the store.");

    /// <summary>Every type mapped for expiration in this process — what the sweeper purges.</summary>
    internal static IReadOnlyCollection<ExpirationMapping> All => (IReadOnlyCollection<ExpirationMapping>)mappings.Values;
}

/// <summary>Maps an expiry property on a document type.</summary>
public static class DocumentExpirationOptionsExtensions
{
    /// <summary>
    /// Maps <paramref name="expiresAt"/> as <typeparamref name="T"/>'s expiry time: once it has passed, the document is
    /// hidden from every read and set-based write (a named query filter, <see cref="DocumentExpiration.FilterName"/>),
    /// until <c>PurgeExpired</c> deletes it. A null expiry never expires. Store expiry times in UTC.
    /// </summary>
    /// <param name="cfg">The type's configuration.</param>
    /// <param name="expiresAt">The nullable expiry property, e.g. <c>x =&gt; x.ExpiresAt</c>.</param>
    /// <param name="defaultTimeToLive">When set, an insert or upsert that leaves the expiry null gets now + this.</param>
    /// <param name="timeProvider">The clock the filter and the default stamp read; <see cref="TimeProvider.System"/> by default.</param>
    /// <example>
    /// <code>
    /// options.ConfigureDocument&lt;Session&gt;(cfg => cfg.MapExpiration(x => x.ExpiresAt, TimeSpan.FromHours(8)));
    /// </code>
    /// </example>
    public static DocumentTypeBuilder<T> MapExpiration<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(
        this DocumentTypeBuilder<T> cfg,
        Expression<Func<T, DateTimeOffset?>> expiresAt,
        TimeSpan? defaultTimeToLive = null,
        TimeProvider? timeProvider = null) where T : class
    {
        ArgumentNullException.ThrowIfNull(cfg);
        DocumentExpiration.Configure<T>(cfg.Options, expiresAt, defaultTimeToLive, timeProvider);
        return cfg;
    }

    /// <inheritdoc cref="MapExpiration{T}(DocumentTypeBuilder{T}, Expression{Func{T, DateTimeOffset?}}, TimeSpan?, TimeProvider?)"/>
    public static DocumentTypeBuilder<T> MapExpiration<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(
        this DocumentTypeBuilder<T> cfg,
        Expression<Func<T, DateTime?>> expiresAt,
        TimeSpan? defaultTimeToLive = null,
        TimeProvider? timeProvider = null) where T : class
    {
        ArgumentNullException.ThrowIfNull(cfg);
        DocumentExpiration.Configure<T>(cfg.Options, expiresAt, defaultTimeToLive, timeProvider);
        return cfg;
    }
}

/// <summary>Read and maintenance helpers for documents mapped with <c>MapExpiration</c>.</summary>
public static class DocumentExpirationExtensions
{
    /// <summary>Lifts the expiration filter for this query so expired documents come back too. Other query filters
    /// stay in force.</summary>
    public static IDocumentQuery<T> IncludeExpired<T>(this IDocumentQuery<T> query) where T : class
    {
        ArgumentNullException.ThrowIfNull(query);
        return query.IgnoreQueryFilters(DocumentExpiration.FilterName);
    }

    /// <summary>
    /// Deletes every expired <typeparamref name="T"/> and returns how many. Runs as a set-based delete, so delete
    /// interceptors — soft delete included — apply as they would to any <c>ExecuteDelete</c>.
    /// </summary>
    public static Task<int> PurgeExpired<T>(this IDocumentStore store, CancellationToken cancellationToken = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(store);

        // The inverse of this store's own filter, so a store built with its own clock purges by that clock. A store
        // that does not expose its mappings (a unit-of-work wrapper) falls back to the type's registered mapping.
        var notExpired = store.GetMappings()?
            .ResolveQueryFilters(typeof(T))
            .FirstOrDefault(f => f.Name == DocumentExpiration.FilterName)?
            .Predicate as Expression<Func<T, bool>>;
        var expired = notExpired != null
            ? Expression.Lambda<Func<T, bool>>(Expression.Not(notExpired.Body), notExpired.Parameters)
            : DocumentExpiration.Require<T>().Expired();

        return store.Query<T>().IncludeExpired().Where(expired).ExecuteDelete(cancellationToken);
    }
}

/// <summary>Registers the background job that purges expired documents.</summary>
public static class DocumentExpirationServiceCollectionExtensions
{
    /// <summary>
    /// Runs <c>PurgeExpired</c> for every type mapped with <c>MapExpiration</c> against the registered
    /// <see cref="IDocumentStore"/> (or the keyed store named <paramref name="storeName"/>) every
    /// <paramref name="interval"/>, starting one interval after the host starts. Failures are logged and retried on
    /// the next tick. Needs a generic host; on MAUI call <c>PurgeExpired</c> yourself (on resume, say).
    /// </summary>
    public static IServiceCollection AddDocumentExpirationSweeper(this IServiceCollection services, TimeSpan interval, string? storeName = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        if (interval <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(interval), interval, "The sweep interval must be positive.");

        services.TryAddSingleton(TimeProvider.System);
        services.AddSingleton<IHostedService>(sp => new DocumentExpirationSweeper(sp, interval, storeName));
        return services;
    }
}

sealed class DocumentExpirationSweeper(IServiceProvider services, TimeSpan interval, string? storeName) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var clock = services.GetService<TimeProvider>() ?? TimeProvider.System;
        var logger = services.GetService<ILoggerFactory>()?.CreateLogger(DocumentStoreLogging.Category);
        using var timer = new PeriodicTimer(interval, clock);
        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
                await this.SweepAsync(logger, stoppingToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // host shutting down
        }
    }

    internal async Task SweepAsync(ILogger? logger, CancellationToken ct)
    {
        await using var scope = services.CreateAsyncScope();
        var store = storeName == null
            ? scope.ServiceProvider.GetRequiredService<IDocumentStore>()
            : scope.ServiceProvider.GetRequiredKeyedService<IDocumentStore>(storeName);

        foreach (var mapping in DocumentExpiration.All)
        {
            try
            {
                var purged = await mapping.PurgeAsync(store, ct).ConfigureAwait(false);
                if (purged > 0)
                    logger?.LogDebug("Purged {Count} expired {Type} documents", purged, mapping.DocumentType.Name);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger?.LogWarning(ex, "Purging expired {Type} documents failed; retrying on the next sweep", mapping.DocumentType.Name);
            }
        }
    }
}

abstract class ExpirationMapping
{
    public abstract Type DocumentType { get; }
    public abstract Task<int> PurgeAsync(IDocumentStore store, CancellationToken ct);
}

// "Now" as a property the filter expression reads on every translation, so the comparison always uses the current
// time rather than the time the filter was built.
sealed class ExpirationClock(TimeProvider timeProvider)
{
    public DateTimeOffset? UtcNowOffset => timeProvider.GetUtcNow();
    public DateTime? UtcNowDateTime => timeProvider.GetUtcNow().UtcDateTime;
    public TimeProvider TimeProvider => timeProvider;
}

sealed class ExpirationMapping<T> : ExpirationMapping where T : class
{
    readonly MemberExpression member;
    readonly ParameterExpression parameter;
    readonly Expression now;
    readonly PropertyInfo property;
    readonly bool isOffset;

    ExpirationMapping(MemberExpression member, ParameterExpression parameter, ExpirationClock clock, PropertyInfo property, bool isOffset, TimeSpan? defaultTimeToLive)
    {
        this.member = member;
        this.parameter = parameter;
        this.property = property;
        this.isOffset = isOffset;
        this.Clock = clock;
        this.DefaultTimeToLive = defaultTimeToLive;
        // Built from lambdas rather than by member name, so the clock's properties survive trimming.
        Expression<Func<DateTimeOffset?>> offsetNow = () => clock.UtcNowOffset;
        Expression<Func<DateTime?>> dateTimeNow = () => clock.UtcNowDateTime;
        this.now = isOffset ? offsetNow.Body : dateTimeNow.Body;

        var nullConstant = Expression.Constant(null, member.Type);
        this.NotExpired = Expression.Lambda<Func<T, bool>>(
            Expression.OrElse(Expression.Equal(member, nullConstant), Expression.GreaterThan(member, this.now)),
            parameter);
    }

    public ExpirationClock Clock { get; }
    public TimeSpan? DefaultTimeToLive { get; }
    public Expression<Func<T, bool>> NotExpired { get; }
    public override Type DocumentType => typeof(T);

    public Expression<Func<T, bool>> Expired()
        => Expression.Lambda<Func<T, bool>>(
            Expression.AndAlso(Expression.NotEqual(this.member, Expression.Constant(null, this.member.Type)), Expression.LessThanOrEqual(this.member, this.now)),
            this.parameter);

    public override Task<int> PurgeAsync(IDocumentStore store, CancellationToken ct) => store.PurgeExpired<T>(ct);

    /// <summary>Stamps now + the default time to live when <paramref name="document"/> has no expiry.</summary>
    public void StampDefault(T document)
    {
        if (this.DefaultTimeToLive is not { } ttl || this.property.GetValue(document) != null)
            return;
        var expires = this.Clock.TimeProvider.GetUtcNow() + ttl;
        this.property.SetValue(document, this.isOffset ? expires : expires.UtcDateTime);
    }

    public static ExpirationMapping<T> Build(LambdaExpression expiresAt, TimeSpan? defaultTimeToLive, TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(expiresAt);
        var body = expiresAt.Body;
        if (body is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } unary)
            body = unary.Operand;
        if (body is not MemberExpression { Member: PropertyInfo property } member || member.Expression is not ParameterExpression)
            throw new ArgumentException(
                $"The expiry must be a top-level property of '{typeof(T).Name}' (e.g. x => x.ExpiresAt).", nameof(expiresAt));
        if (member.Type != typeof(DateTimeOffset?) && member.Type != typeof(DateTime?))
            throw new ArgumentException(
                $"The expiry must be a nullable DateTimeOffset or DateTime; '{typeof(T).Name}.{property.Name}' is a {member.Type.Name}.", nameof(expiresAt));
        if (defaultTimeToLive != null && !property.CanWrite)
            throw new ArgumentException(
                $"'{typeof(T).Name}.{property.Name}' has no setter, so a default time to live cannot be stamped on it.", nameof(expiresAt));

        return new ExpirationMapping<T>(member, expiresAt.Parameters[0], new ExpirationClock(timeProvider), property, member.Type == typeof(DateTimeOffset?), defaultTimeToLive);
    }
}

// Stamps the default expiry before an insert or upsert is serialized.
sealed class ExpirationInterceptor<T>(ExpirationMapping<T> mapping) : IDocumentInterceptor where T : class
{
    public int Order => int.MinValue;

    public Task BeforeWrite(DocumentWriteContext ctx, CancellationToken ct)
    {
        if (ctx.Operation is DocumentOperation.Insert or DocumentOperation.Upsert && ctx.Document is T document)
            mapping.StampDefault(document);
        return Task.CompletedTask;
    }

    public Task AfterWrite(DocumentWriteContext ctx, CancellationToken ct) => Task.CompletedTask;
}
