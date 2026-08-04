using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans;
using Orleans.Runtime;
using Orleans.Storage;

namespace Shiny.DocumentDb.Orleans;

/// <summary>
/// An Orleans <see cref="IGrainStorage"/> implemented entirely against the backend-agnostic
/// <see cref="IDocumentStore"/> abstraction. The same code runs on every Shiny.DocumentDb provider; which
/// backends are production-suitable is a documentation concern (see the package readme), not a code branch.
/// </summary>
public sealed class DocumentDbGrainStorage : IGrainStorage, ILifecycleParticipant<ISiloLifecycle>
{
    readonly string name;
    readonly DocumentDbGrainStorageOptions options;
    readonly ILogger<DocumentDbGrainStorage> logger;
    readonly IDocumentStore store;
    readonly JsonSerializerOptions jsonOptions;

    public DocumentDbGrainStorage(
        string name,
        DocumentDbGrainStorageOptions options,
        IServiceProvider services,
        ILogger<DocumentDbGrainStorage> logger
    )
    {
        this.name = name;
        this.options = options;
        this.logger = logger;
        this.jsonOptions = options.JsonSerializerOptions ?? new JsonSerializerOptions(JsonSerializerDefaults.General);

        this.store = options.StoreFactory is not null
            ? options.StoreFactory(services)
            : BuildRelationalStore(options, name, services);
    }

    internal static DocumentDbGrainStorage Create(IServiceProvider services, string name)
    {
        var options = services
            .GetRequiredService<IOptionsMonitor<DocumentDbGrainStorageOptions>>()
            .Get(name);

        return ActivatorUtilities.CreateInstance<DocumentDbGrainStorage>(services, name, options);
    }

    static IDocumentStore BuildRelationalStore(DocumentDbGrainStorageOptions o, string name, IServiceProvider services)
    {
        if (o.DatabaseProvider is null)
            throw new InvalidOperationException(
                $"Grain storage provider '{name}' has neither a DatabaseProvider nor a StoreFactory configured.");

        var tableName = o.TableName ?? $"orleans_{name.ToLowerInvariant()}";
        var dso = new DocumentStoreOptions
        {
            DatabaseProvider = o.DatabaseProvider,
            // The GrainStateRecord envelope is source-generated, so the store serializes it reflection-free.
            // The grain state itself is (de)serialized separately via `jsonOptions` (see ResolveStateTypeInfo).
            JsonSerializerOptions = OrleansSystemJson.StoreOptions(o.JsonSerializerOptions),
            UseReflectionFallback = o.UseReflectionFallback
        };

        ConfigureGrainState(dso, tableName);
        // Pass the silo services so DI-registered interceptors fire on grain writes; a scoped interceptor
        // resolves from a fresh child scope opened per unit when no ambient session scope flows.
        return new DocumentStore(dso, services);
    }

    /// <summary>
    /// Applies the <see cref="GrainStateRecord"/> mappings (dedicated table + version property) that the
    /// grain-storage provider requires, to a relational <see cref="DocumentStoreOptions"/>. Exposed so a
    /// <see cref="DocumentDbGrainStorageOptions.StoreFactory"/> built on the relational core can reuse it.
    /// </summary>
    public static void ConfigureGrainState(DocumentStoreOptions options, string tableName)
    {
        options.ConfigureDocument<GrainStateRecord>(cfg =>
        {
            cfg.Table = tableName;
            cfg.MapVersionProperty(x => x.Version);
        });
    }

    // GrainId.ToString() renders as "{grainType}/{key}" — the '/' (and '\', '?', '#') are reserved characters
    // in a Cosmos DB item id, breaking the point-read on ReadStateAsync. Percent-encode them (and '%' first,
    // to keep the encoding unambiguous) so the composite id is valid on Cosmos and unchanged in meaning on the
    // relational/Mongo providers.
    static string MakeId(string stateName, GrainId grainId)
        => $"{stateName}|{SanitizeId(grainId.ToString())}";

    static string SanitizeId(string s) => s
        .Replace("%", "%25")
        .Replace("/", "%2F")
        .Replace("\\", "%5C")
        .Replace("?", "%3F")
        .Replace("#", "%23");

    static int ParseEtag(string etag) => int.Parse(etag, CultureInfo.InvariantCulture);

    /// <summary>JSON <c>null</c> tombstone — built without a serializer so it needs no type metadata.</summary>
    static readonly JsonElement NullState = JsonDocument.Parse("null").RootElement.Clone();

    /// <summary>
    /// Resolves a source-generated <see cref="JsonTypeInfo{T}"/> for the grain-state type from the
    /// configured resolver. Returns <c>null</c> to take the reflection fallback when
    /// <see cref="DocumentDbGrainStorageOptions.UseReflectionFallback"/> is enabled; otherwise throws.
    /// </summary>
    JsonTypeInfo<T>? ResolveStateTypeInfo<T>()
    {
        if (this.jsonOptions.TryGetTypeInfo(typeof(T), out var info) && info is JsonTypeInfo<T> typed)
            return typed;

        if (!this.options.UseReflectionFallback)
            throw new InvalidOperationException(
                $"No JsonTypeInfo registered for grain-state type '{typeof(T).FullName}'. " +
                "Register it in a JsonSerializerContext assigned to " +
                "DocumentDbGrainStorageOptions.JsonSerializerOptions, or leave UseReflectionFallback = true.");

        return null;
    }

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Reflection path only used when no JsonTypeInfo is resolved and UseReflectionFallback is true.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Reflection path only used when no JsonTypeInfo is resolved and UseReflectionFallback is true.")]
    static JsonElement StateToElement<T>(T value, JsonTypeInfo<T>? typeInfo, JsonSerializerOptions options)
        => typeInfo != null
            ? JsonSerializer.SerializeToElement(value, typeInfo)
            : JsonSerializer.SerializeToElement(value, options);

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Reflection path only used when no JsonTypeInfo is resolved and UseReflectionFallback is true.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Reflection path only used when no JsonTypeInfo is resolved and UseReflectionFallback is true.")]
    static T? StateFromElement<T>(JsonElement element, JsonTypeInfo<T>? typeInfo, JsonSerializerOptions options)
        => typeInfo != null
            ? element.Deserialize(typeInfo)
            : element.Deserialize<T>(options);

    public async Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        var id = MakeId(stateName, grainId);
        var rec = await this.store.Get<GrainStateRecord>(id).ConfigureAwait(false);

        if (rec is null)
        {
            // Orleans pre-populates grainState.State with a fresh instance; leave it untouched.
            grainState.RecordExists = false;
            grainState.ETag = null!;
            return;
        }

        grainState.State = StateFromElement(rec.State, this.ResolveStateTypeInfo<T>(), this.jsonOptions)!;
        grainState.ETag = rec.Version.ToString(CultureInfo.InvariantCulture);
        grainState.RecordExists = true;
    }

    public async Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        var id = MakeId(stateName, grainId);
        var stateJson = StateToElement(grainState.State, this.ResolveStateTypeInfo<T>(), this.jsonOptions);

        try
        {
            if (String.IsNullOrEmpty(grainState.ETag))
            {
                // New state — Insert assigns Version = 1.
                var rec = new GrainStateRecord { Id = id, State = stateJson };
                await this.store.Insert(rec).ConfigureAwait(false);
                grainState.ETag = rec.Version.ToString(CultureInfo.InvariantCulture);
            }
            else
            {
                // Existing state — Update CAS-checks Version against the stored row and increments it.
                var rec = new GrainStateRecord { Id = id, Version = ParseEtag(grainState.ETag), State = stateJson };
                await this.store.Update(rec).ConfigureAwait(false);
                grainState.ETag = rec.Version.ToString(CultureInfo.InvariantCulture);
            }
            grainState.RecordExists = true;
        }
        catch (ConcurrencyException ex)
        {
            throw new InconsistentStateException(
                $"Optimistic concurrency violation writing grain state '{stateName}' for {grainId} " +
                $"(provider '{this.name}').",
                ex);
        }
        catch (InvalidOperationException ex)
        {
            // Insert of an already-existing row (duplicate activation) or Update of a vanished row.
            throw new InconsistentStateException(
                $"Inconsistent state writing grain state '{stateName}' for {grainId} (provider '{this.name}').",
                ex);
        }
    }

    public async Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        var id = MakeId(stateName, grainId);

        if (this.options.DeleteStateOnClear)
        {
            await this.store.Remove<GrainStateRecord>(id).ConfigureAwait(false);
            grainState.ETag = null!;
            grainState.RecordExists = false;
            return;
        }

        // Tombstone: keep the row, write null state, advance the version chain.
        var nullState = NullState;
        try
        {
            if (String.IsNullOrEmpty(grainState.ETag))
            {
                var rec = new GrainStateRecord { Id = id, State = nullState };
                await this.store.Insert(rec).ConfigureAwait(false);
                grainState.ETag = rec.Version.ToString(CultureInfo.InvariantCulture);
            }
            else
            {
                var rec = new GrainStateRecord { Id = id, Version = ParseEtag(grainState.ETag), State = nullState };
                await this.store.Update(rec).ConfigureAwait(false);
                grainState.ETag = rec.Version.ToString(CultureInfo.InvariantCulture);
            }
            grainState.RecordExists = true;
        }
        catch (ConcurrencyException ex)
        {
            throw new InconsistentStateException(
                $"Optimistic concurrency violation clearing grain state '{stateName}' for {grainId} " +
                $"(provider '{this.name}').",
                ex);
        }
    }

    public void Participate(ISiloLifecycle lifecycle) =>
        lifecycle.Subscribe(
            OptionFormattingUtilities.Name<DocumentDbGrainStorage>(this.name),
            this.options.InitStage,
            this.InitAsync,
            _ => Task.CompletedTask);

    async Task InitAsync(CancellationToken ct)
    {
        // Force schema/connection initialization so the silo fails fast on a misconfigured store.
        await this.store.Count<GrainStateRecord>(cancellationToken: ct).ConfigureAwait(false);
        this.logger.LogInformation("DocumentDb grain storage '{Name}' initialized.", this.name);
    }
}
