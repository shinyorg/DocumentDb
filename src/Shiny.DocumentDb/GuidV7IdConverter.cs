namespace Shiny.DocumentDb;

/// <summary>
/// A built-in <see cref="DocumentIdConverter{TId}"/> for <see cref="Guid"/> Ids that auto-generates
/// <b>version 7</b> (time-ordered) GUIDs via <see cref="Guid.CreateVersion7()"/> instead of the default
/// random version 4. v7 Ids sort chronologically, which keeps inserts append-friendly for indexes.
/// <para>
/// Storage format is unchanged from the default Guid handling (32-char "N", no dashes), so it is a
/// drop-in for existing Guid-keyed data — only newly generated Ids differ. Register it with
/// <c>options.UseGuidV7Ids()</c> or <c>options.MapIdType(new GuidV7IdConverter())</c>. Depends only on the BCL.
/// </para>
/// </summary>
public sealed class GuidV7IdConverter : DocumentIdConverter<Guid>
{
    public override string ToStorageString(Guid id) => id.ToString("N");
    public override Guid FromStorageString(string id) => Guid.Parse(id);
    public override bool IsDefault(Guid id) => id == Guid.Empty;
    public override bool TryGenerate(out Guid id)
    {
        id = Guid.CreateVersion7();
        return true;
    }
}
