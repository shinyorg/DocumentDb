namespace Shiny.DocumentDb.RavenDb;

/// <summary>
/// The storage envelope persisted in RavenDB. Every document — regardless of its CLR type — is stored as
/// one of these wrappers in the single <c>RavenDbDocuments</c> collection. The caller's POCO is serialized
/// by <b>our</b> System.Text.Json pipeline into the opaque <see cref="DataJson"/> string, so RavenDB's own
/// (Newtonsoft-based) serializer never reinterprets the document body — round-trips stay byte-for-byte
/// deterministic and consistent with every other provider.
/// <para>
/// The property is deliberately named <see cref="DocId"/> (not <c>Id</c>) so RavenDB's identity convention
/// does not hijack it; the RavenDB document id is supplied explicitly as
/// <c>{typeName}/{logicalId}</c> (see <see cref="RavenId"/>).
/// </para>
/// </summary>
class RavenDbDocument
{
    /// <summary>The logical document id (the value of the POCO's Id property, in storage-string form).</summary>
    public string DocId { get; set; } = "";

    /// <summary>The resolved document type name (collection discriminator within the single Raven collection).</summary>
    public string TypeName { get; set; } = "";

    /// <summary>The caller's POCO serialized with our JsonSerializerOptions — stored AS-IS (opaque to Raven).</summary>
    public string DataJson { get; set; } = "";

    public DateTime CreatedAt { get; set; }
    public DateTime UpdatedAt { get; set; }

    /// <summary>Mirror of the mapped optimistic-concurrency version (when <c>MapVersionProperty</c> is used).</summary>
    public int? Version { get; set; }

    /// <summary>The RavenDB document id for a logical (typeName, id) pair. A <c>/</c> separator keeps ids of
    /// different types isolated within the single collection and never ends in <c>/</c> or <c>|</c> (which
    /// would trigger server-side id generation).</summary>
    public static string RavenId(string typeName, string id) => $"{typeName}/{id}";

    /// <summary>The RavenDB id-prefix that selects every document of a type (for prefix streaming).</summary>
    public static string Prefix(string typeName) => $"{typeName}/";
}
