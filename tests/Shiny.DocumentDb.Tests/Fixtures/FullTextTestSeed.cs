namespace Shiny.DocumentDb.Tests.Fixtures;

/// <summary>Shared document + corpus used by the cross-provider full-text search tests.</summary>
public class FtArticle
{
    public string Id { get; set; } = "";
    public string Title { get; set; } = "";
    public string Body { get; set; } = "";
    public string Category { get; set; } = "";
}

public static class FullTextTestSeed
{
    public static readonly FtArticle[] Docs =
    [
        new() { Id = "1", Title = "Orleans persistence", Body = "Using DocumentDb as an Orleans grain storage provider.", Category = "tech" },
        new() { Id = "2", Title = "Cooking with garlic", Body = "A recipe that uses a lot of garlic and onion.", Category = "food" },
        new() { Id = "3", Title = "Distributed systems", Body = "Orleans is a virtual actor framework for distributed applications.", Category = "tech" }
    ];
}
