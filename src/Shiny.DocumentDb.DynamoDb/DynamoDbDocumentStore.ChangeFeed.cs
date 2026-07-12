using System.Text.Json.Serialization.Metadata;
using Amazon.DynamoDBStreams;
using Amazon.DynamoDBStreams.Model;
using Amazon.Runtime;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.DynamoDb;

// Native change feed backed by DynamoDB Streams. Kept in its own file because the Streams model types
// (AttributeValue, Record, …) live in Amazon.DynamoDBStreams.Model and would clash with the DynamoDBv2
// model types used by the main store file.
public partial class DynamoDbDocumentStore
{
    IAmazonDynamoDBStreams? streamsClient;
    readonly object streamsClientLock = new();

    IAmazonDynamoDBStreams StreamsClient()
    {
        // Double-checked lock: concurrent SubscribeChanges calls must not each build (and leak) a client.
        if (this.streamsClient != null)
            return this.streamsClient;

        lock (this.streamsClientLock)
        {
            if (this.streamsClient != null)
                return this.streamsClient;

            var config = new AmazonDynamoDBStreamsConfig();
            var serviceUrl = this.options.ServiceUrl;
            var region = this.options.Region;
            if (string.IsNullOrEmpty(serviceUrl) && region == null && this.client is AmazonServiceClient asc)
            {
                serviceUrl = asc.Config.ServiceURL;
                region = asc.Config.RegionEndpoint;
            }
            if (!string.IsNullOrEmpty(serviceUrl))
                config.ServiceURL = serviceUrl;
            else if (region != null)
                config.RegionEndpoint = region;

            IAmazonDynamoDBStreams client;
            if (this.options.Credentials != null)
                client = new AmazonDynamoDBStreamsClient(this.options.Credentials, config);
            else if (!string.IsNullOrEmpty(serviceUrl))
                client = new AmazonDynamoDBStreamsClient(new BasicAWSCredentials("dummy", "dummy"), config);
            else
                client = new AmazonDynamoDBStreamsClient(config);

            this.streamsClient = client;   // publish only after fully constructed
            return client;
        }
    }

    /// <inheritdoc />
    /// <remarks>Backed by DynamoDB Streams (the table's stream is enabled automatically when the store
    /// creates it). Delivers inserts, updates, and deletes made by any writer. Requires a table with a
    /// stream enabled (<c>NEW_AND_OLD_IMAGES</c> or <c>NEW_IMAGE</c>).</remarks>
    public async Task<IAsyncDisposable> SubscribeChanges<T>(
        Func<DocumentChange<T>, CancellationToken, Task> onChange,
        CancellationToken cancellationToken = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(onChange);
        var typeInfo = this.FindTypeInfo<T>(null);
        var partitionKey = this.ResolvePartitionKey<T>();
        await this.EnsureTableAsync(cancellationToken).ConfigureAwait(false);

        var desc = await this.client.DescribeTableAsync(this.TableName, cancellationToken).ConfigureAwait(false);
        var streamArn = desc.Table.LatestStreamArn
            ?? throw new NotSupportedException(
                $"DynamoDB table '{this.TableName}' has no stream enabled. Enable a stream (NEW_AND_OLD_IMAGES) to use SubscribeChanges.");

        var streams = this.StreamsClient();
        return new ChangeFeedSubscription(cancellationToken,
            token => this.RunStreamAsync(streams, streamArn, partitionKey, typeInfo, onChange, token));
    }

    async Task RunStreamAsync<T>(
        IAmazonDynamoDBStreams streams,
        string streamArn,
        string partitionKey,
        JsonTypeInfo<T>? typeInfo,
        Func<DocumentChange<T>, CancellationToken, Task> onChange,
        CancellationToken token) where T : class
    {
        // shardId -> current iterator (null once the shard is exhausted).
        var iterators = new Dictionary<string, string?>();
        var idleDelay = TimeSpan.FromMilliseconds(750);

        try
        {
            // The initial shards start at LATEST — SubscribeChanges is a live feed, not a history replay.
            await this.RefreshShardsAsync(streams, streamArn, iterators, initial: true, token).ConfigureAwait(false);

            while (!token.IsCancellationRequested)
            {
                foreach (var shardId in iterators.Keys.ToList())
                {
                    var iterator = iterators[shardId];
                    if (iterator == null)
                        continue;

                    GetRecordsResponse records;
                    try
                    {
                        records = await streams.GetRecordsAsync(new GetRecordsRequest { ShardIterator = iterator }, token).ConfigureAwait(false);
                    }
                    catch (ExpiredIteratorException)
                    {
                        iterators[shardId] = null;
                        continue;
                    }

                    foreach (var record in records.Records)
                        await this.DispatchAsync(record, partitionKey, typeInfo, onChange, token).ConfigureAwait(false);

                    // NextShardIterator is null once a closed (split/rolled) shard is fully drained.
                    iterators[shardId] = records.NextShardIterator;
                }

                // Pick up shard splits/rolls (children start at TRIM_HORIZON, and only once their parent has
                // drained), then idle before the next poll.
                await this.RefreshShardsAsync(streams, streamArn, iterators, initial: false, token).ConfigureAwait(false);
                await Task.Delay(idleDelay, token).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) { }
    }

    async Task RefreshShardsAsync(IAmazonDynamoDBStreams streams, string streamArn, Dictionary<string, string?> iterators, bool initial, CancellationToken token)
    {
        var desc = await streams.DescribeStreamAsync(new DescribeStreamRequest { StreamArn = streamArn }, token).ConfigureAwait(false);
        foreach (var shard in desc.StreamDescription.Shards)
        {
            if (iterators.ContainsKey(shard.ShardId))
                continue;

            // A shard that appears after the initial subscribe is a split/roll child. Two rules keep it correct:
            //  1. Read it from TRIM_HORIZON (not LATEST) so records written to it before we discovered it aren't
            //     skipped — LATEST would drop everything between the split and this refresh.
            //  2. Don't begin it until its parent shard has drained (its iterator went null / is no longer
            //     tracked), so per-key ordering is preserved across the split.
            if (!initial)
            {
                var parentId = shard.ParentShardId;
                var parentStillDraining = parentId != null
                    && iterators.TryGetValue(parentId, out var parentIterator)
                    && parentIterator != null;
                if (parentStillDraining)
                    continue; // revisit on a later refresh once the parent finishes
            }

            var iteratorType = initial ? ShardIteratorType.LATEST : ShardIteratorType.TRIM_HORIZON;
            var it = await streams.GetShardIteratorAsync(new GetShardIteratorRequest
            {
                StreamArn = streamArn,
                ShardId = shard.ShardId,
                ShardIteratorType = iteratorType
            }, token).ConfigureAwait(false);
            iterators[shard.ShardId] = it.ShardIterator;
        }
    }

    async Task DispatchAsync<T>(
        Record record,
        string partitionKey,
        JsonTypeInfo<T>? typeInfo,
        Func<DocumentChange<T>, CancellationToken, Task> onChange,
        CancellationToken token) where T : class
    {
        var dynamo = record.Dynamodb;
        if (dynamo?.Keys == null || !dynamo.Keys.TryGetValue(DynamoDbDocument.Pk, out var pk) || pk.S != partitionKey)
            return;

        var id = dynamo.Keys.TryGetValue(DynamoDbDocument.Sk, out var sk) ? sk.S : "";

        DocumentChangeType changeType;
        T? document = null;
        if (record.EventName == OperationType.REMOVE)
        {
            changeType = DocumentChangeType.Removed;
        }
        else
        {
            changeType = record.EventName == OperationType.INSERT ? DocumentChangeType.Inserted : DocumentChangeType.Updated;
            if (dynamo.NewImage != null && dynamo.NewImage.TryGetValue(DynamoDbDocument.DataAttr, out var data) && data.S != null)
                document = Deserialize(data.S, typeInfo, this.JsonOptions);
        }

        await onChange(new DocumentChange<T> { ChangeType = changeType, Id = id, Document = document }, token).ConfigureAwait(false);
    }
}
