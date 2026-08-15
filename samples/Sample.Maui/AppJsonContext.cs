using System.Text.Json.Serialization;

namespace Sample.Maui;

[JsonSerializable(typeof(Customer))]
[JsonSerializable(typeof(Order))]
[JsonSerializable(typeof(OrderLine))]
[JsonSerializable(typeof(List<OrderLine>))]
[JsonSerializable(typeof(VectorNote))]
[JsonSerializable(typeof(GeofenceZone))]
[JsonSerializable(typeof(Landmark))]
public partial class AppJsonContext : JsonSerializerContext;
