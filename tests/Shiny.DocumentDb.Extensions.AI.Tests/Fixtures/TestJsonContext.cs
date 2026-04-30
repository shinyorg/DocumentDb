using System.Text.Json.Serialization;

namespace Shiny.DocumentDb.Extensions.AI.Tests.Fixtures;

[JsonSerializable(typeof(Customer))]
[JsonSerializable(typeof(Product))]
public partial class TestJsonContext : JsonSerializerContext;
