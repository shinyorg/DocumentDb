using System.Text.Json.Serialization;

namespace Shiny.DocumentDb.Tests.Fixtures;

[JsonSerializable(typeof(User))]
[JsonSerializable(typeof(Product))]
[JsonSerializable(typeof(Event))]
[JsonSerializable(typeof(Order))]
[JsonSerializable(typeof(Address))]
[JsonSerializable(typeof(OrderLine))]
[JsonSerializable(typeof(UserSummary))]
[JsonSerializable(typeof(OrderSummary))]
[JsonSerializable(typeof(OrderDetail))]
[JsonSerializable(typeof(OrderLineAggregates))]
[JsonSerializable(typeof(OrderStats))]
[JsonSerializable(typeof(PriceSummary))]
[JsonSerializable(typeof(GuidIdModel))]
[JsonSerializable(typeof(IntIdModel))]
[JsonSerializable(typeof(LongIdModel))]
[JsonSerializable(typeof(StringIdModel))]
public partial class TestJsonContext : JsonSerializerContext;
