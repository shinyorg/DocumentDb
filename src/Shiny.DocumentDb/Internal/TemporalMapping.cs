namespace Shiny.DocumentDb.Internal;

class TemporalMapping
{
    public required Type DocumentType { get; init; }
    public TimeSpan? Retention { get; init; }
    public int? MaxVersions { get; init; }
    public Func<string?>? CaptureActor { get; init; }
}
