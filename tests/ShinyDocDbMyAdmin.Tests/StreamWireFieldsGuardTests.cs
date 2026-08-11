using System.Text.Json;
using ShinyDocDbMyAdmin.Models;
using Shiny.DocumentDb.Orleans;
using Xunit;

namespace ShinyDocDbMyAdmin.Tests;

/// <summary>
/// Holds the admin tool's copy of the stream wire names in step with the library's.
/// </summary>
/// <remarks>
/// The tool addresses stream rows through the schema-free JSON-collection lane using string field paths,
/// because taking a project reference on <c>Shiny.DocumentDb.Orleans</c> would pull the Orleans runtime into
/// a tool that already ships ~152 MB of native database providers — for a screen that counts rows and reads
/// two timestamps. The cost of that decision is a duplicated set of constants, and the price of the
/// duplication is this test: rename a property in the library without updating the tool and the streams
/// screen would silently show zeroes, which is the worst possible failure for a screen whose entire job is
/// to tell you whether anything is stuck.
/// </remarks>
public class StreamWireFieldsGuardTests
{
    [Fact]
    public void AdminWireNames_MatchTheLibrarys()
    {
        Assert.Equal(StreamFields.Id, StreamWireFields.Id);
        Assert.Equal(StreamFields.QueueId, StreamWireFields.QueueId);
        Assert.Equal(StreamFields.Seq, StreamWireFields.Seq);
        Assert.Equal(StreamFields.StreamId, StreamWireFields.StreamId);
        Assert.Equal(StreamFields.StreamNamespace, StreamWireFields.StreamNamespace);
        Assert.Equal(StreamFields.Payload, StreamWireFields.Payload);
        Assert.Equal(StreamFields.EnqueuedAt, StreamWireFields.EnqueuedAt);
        Assert.Equal(StreamFields.DeliveredAt, StreamWireFields.DeliveredAt);
    }

    [Fact]
    public void AdminTypeNames_MatchTheLibrarys()
    {
        // Discovery matches on these, so a rename here means the screen finds nothing at all rather than
        // finding it and showing it wrong.
        Assert.Equal(StreamFields.EventTypeName, StreamWireFields.EventTypeName);
        Assert.Equal(StreamFields.SequenceTypeName, StreamWireFields.SequenceTypeName);
        Assert.Equal(StreamFields.CheckpointTypeName, StreamWireFields.CheckpointTypeName);
    }

    [Fact]
    public void PinnedNames_AreWhatIsActuallySerialized()
    {
        // The constants are only worth anything if the document really serializes to them. [JsonPropertyName]
        // pins each one, so this also proves a future naming-policy change cannot move the stored keys out
        // from under the admin tool.
        var json = JsonSerializer.Serialize(new StreamEventDocument
        {
            Id = "e1",
            QueueId = "q1",
            Seq = 7,
            StreamId = "ns/key",
            StreamNamespace = "ns",
            Payload = "AAAA",
            EnqueuedAt = DateTimeOffset.UnixEpoch,
            DeliveredAt = DateTimeOffset.UnixEpoch
        });

        using var document = JsonDocument.Parse(json);
        var keys = document.RootElement.EnumerateObject().Select(p => p.Name).ToHashSet(StringComparer.Ordinal);

        foreach (var field in StreamFields.All)
            Assert.Contains(field, keys);
    }
}
