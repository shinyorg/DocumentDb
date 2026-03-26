using System.Text.Json.Serialization;

namespace Shiny.DocumentDb.Benchmarks.Providers;

public class BenchmarkUser
{
    public string Id { get; set; } = "";
    public string Name { get; set; } = "";
    public int Age { get; set; }
    public string? Email { get; set; }
}

public class EfUser
{
    public int Id { get; set; }
    public string Name { get; set; } = "";
    public int Age { get; set; }
    public string? Email { get; set; }
}

[JsonSerializable(typeof(BenchmarkUser))]
public partial class BenchmarkJsonContext : JsonSerializerContext;
