namespace Shiny.DocumentDb.IndexedDb;

internal class DocumentRecord
{
    public string Key { get; set; } = default!;
    public string Id { get; set; } = default!;
    public string TypeName { get; set; } = default!;
    public string Data { get; set; } = default!;
    public string CreatedAt { get; set; } = default!;
    public string UpdatedAt { get; set; } = default!;
}
