using System.Text.Json.Serialization;

namespace Sample;

[JsonSerializable(typeof(Customer))]
[JsonSerializable(typeof(Order))]
[JsonSerializable(typeof(Address))]
[JsonSerializable(typeof(OrderLine))]
[JsonSerializable(typeof(OrderSummary))]
[JsonSerializable(typeof(OrderStats))]
[JsonSerializable(typeof(Sensor))]
public partial class SampleJsonContext : JsonSerializerContext;
