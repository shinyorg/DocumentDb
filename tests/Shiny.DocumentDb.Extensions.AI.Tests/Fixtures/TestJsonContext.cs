using System.Text.Json.Serialization;

namespace Shiny.DocumentDb.Extensions.AI.Tests.Fixtures;

[JsonSerializable(typeof(Customer))]
[JsonSerializable(typeof(Product))]
[JsonSerializable(typeof(Ticket))]
[JsonSerializable(typeof(Counter))]
public partial class TestJsonContext : JsonSerializerContext;
