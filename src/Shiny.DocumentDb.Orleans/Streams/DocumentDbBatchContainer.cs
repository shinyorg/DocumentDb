using global::Orleans;
using global::Orleans.Providers.Streams.Common;
using global::Orleans.Runtime;
using global::Orleans.Streams;

namespace Shiny.DocumentDb.Orleans;

/// <summary>
/// One enqueued batch of events, as Orleans sees it. Serialized whole by Orleans' own serializer and stored
/// as the payload of a <see cref="StreamEventDocument"/>, so event types follow Orleans' versioning rules
/// rather than System.Text.Json's.
/// </summary>
[GenerateSerializer]
public sealed class DocumentDbBatchContainer : IBatchContainer
{
    [Id(0)]
    public StreamId StreamId { get; set; }

    [Id(1)]
    public List<object> Events { get; set; } = [];

    [Id(2)]
    public Dictionary<string, object>? RequestContext { get; set; }

    /// <summary>The queue position assigned at enqueue. Not serialized as the token itself — the token is
    /// rebuilt on read so a batch read back from storage carries the same sequence it was written with.</summary>
    [Id(3)]
    public long SequenceNumber { get; set; }

    public StreamSequenceToken SequenceToken => new EventSequenceTokenV2(this.SequenceNumber);

    /// <summary>
    /// Events of the requested type with a per-event token. Filtering by type before indexing (rather than
    /// casting every element) matches Orleans' own batch containers: a consumer subscribed to one event type
    /// on a mixed-type stream gets its events, not a cast exception.
    /// </summary>
    public IEnumerable<Tuple<T, StreamSequenceToken>> GetEvents<T>()
        => this.Events
            .OfType<T>()
            .Select((e, i) => Tuple.Create<T, StreamSequenceToken>(e, new EventSequenceTokenV2(this.SequenceNumber, i)));

    public bool ImportRequestContext()
    {
        if (this.RequestContext == null)
            return false;

        RequestContextExtensions.Import(this.RequestContext);
        return true;
    }

    public override string ToString()
        => $"[DocumentDbBatchContainer:Stream={this.StreamId},#Items={this.Events.Count},Seq={this.SequenceNumber}]";
}
