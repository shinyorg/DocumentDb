using System.Text.Json.Serialization;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb;

/// <summary>
/// Store-owned facts about a document — when it was first written and when it last changed. Declare one
/// property of this type on a document and the store fills it in on every read and after every write:
/// <code>
/// public class Order
/// {
///     public string Id { get; set; } = "";
///     public DocumentMetadata? Metadata { get; set; }
/// }
/// </code>
/// </summary>
/// <remarks>
/// <para>
/// No mapping call is needed — the property is found by its type. The values come from the timestamps every
/// provider already keeps on the stored envelope (the <c>CreatedAt</c>/<c>UpdatedAt</c> columns on the relational
/// providers, the envelope fields elsewhere); they are never written into the document body, so there is one
/// source of truth and nothing to drift.
/// </para>
/// <para>
/// The store <b>news the object up</b> when the property is null, so a document the store hands back never has a
/// null <c>Metadata</c>. That is why the property must be assignable (<c>set</c>, <c>init</c>, or a non-public
/// setter marked <c>[JsonInclude]</c>). The members below have internal setters — application code reads them,
/// only the store writes them.
/// </para>
/// <para>
/// Queryable: <c>Where(x =&gt; x.Metadata!.UpdatedAt &lt; cutoff)</c> and <c>OrderBy(x =&gt; x.Metadata!.CreatedAt)</c>
/// run against the envelope, not the body.
/// </para>
/// </remarks>
[JsonConverter(typeof(DocumentMetadataJsonConverter))]
public sealed class DocumentMetadata
{
    /// <summary>When the document was first written.</summary>
    public DateTimeOffset CreatedAt { get; internal set; }

    /// <summary>When the document was last written.</summary>
    public DateTimeOffset UpdatedAt { get; internal set; }

    /// <summary>
    /// True once the store has stamped this instance — on a read, or after a successful write. False on an
    /// instance application code constructed itself, and on temporal snapshots, which carry no envelope.
    /// </summary>
    public bool IsPersisted { get; internal set; }
}
