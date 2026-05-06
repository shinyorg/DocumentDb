namespace Shiny.DocumentDb.Internal;

class VersionMapping
{
    public required Type DocumentType { get; init; }
    public required string PropertyName { get; init; }
    public string JsonPath { get; set; } = null!;
    public required Func<object, int> GetVersion { get; init; }
    public required Action<object, int> SetVersion { get; init; }
}
