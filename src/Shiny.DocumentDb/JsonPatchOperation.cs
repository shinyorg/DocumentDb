using System.Text.Json;

namespace Shiny.DocumentDb;

public sealed class JsonPatchOperation
{
    public string Op { get; }
    public string Path { get; }
    public string? From { get; }
    public JsonElement? Value { get; }

    JsonPatchOperation(string op, string path, string? from, JsonElement? value)
    {
        Op = op;
        Path = path;
        From = from;
        Value = value;
    }

    public static JsonPatchOperation Add(string path, JsonElement? value)
        => new("add", path, null, value);

    public static JsonPatchOperation Replace(string path, JsonElement? value)
        => new("replace", path, null, value);

    public static JsonPatchOperation Remove(string path)
        => new("remove", path, null, null);

    public static JsonPatchOperation Copy(string from, string path)
        => new("copy", path, from, null);

    public static JsonPatchOperation Move(string from, string path)
        => new("move", path, from, null);

    public static JsonPatchOperation Test(string path, JsonElement? value)
        => new("test", path, null, value);
}
