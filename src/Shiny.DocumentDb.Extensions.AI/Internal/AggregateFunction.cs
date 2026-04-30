using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.Extensions.AI;

namespace Shiny.DocumentDb.Extensions.AI.Internal;

sealed class AggregateFunction<T> : DocumentAIFunctionBase<T> where T : class
{
    public AggregateFunction(
        IDocumentStore store,
        DocumentAITypeRegistration<T> registration,
        IReadOnlyList<DocumentField> fields,
        string name,
        string description,
        JsonElement schema)
        : base(store, registration, fields, name, description, schema) { }

    protected override async ValueTask<object?> InvokeCoreAsync(
        AIFunctionArguments arguments,
        CancellationToken cancellationToken)
    {
        var function = GetArg<string>(arguments, "function")
            ?? throw new InvalidOperationException("'function' argument is required.");
        var fieldName = GetArg<string>(arguments, "field");
        arguments.TryGetValue("filter", out var filterRaw);
        var filter = filterRaw is JsonElement je ? (JsonElement?)je : null;

        var query = this.Store.Query(this.Registration.JsonTypeInfo);
        var predicate = FilterTranslator.Translate<T>(filter, this.Fields);
        if (predicate != null)
            query = query.Where(predicate);

        if (string.Equals(function, "count", StringComparison.OrdinalIgnoreCase))
        {
            var count = await query.Count(cancellationToken).ConfigureAwait(false);
            return new { function = "count", value = count };
        }

        if (string.IsNullOrWhiteSpace(fieldName))
            throw new InvalidOperationException($"Aggregate '{function}' requires a 'field' argument.");

        var prop = ResolveNumericField(fieldName!);
        var result = function.ToLowerInvariant() switch
        {
            "sum" => await DispatchSumAsync(query, prop, cancellationToken).ConfigureAwait(false),
            "min" => await DispatchMinMaxAsync(query, prop, isMin: true, cancellationToken).ConfigureAwait(false),
            "max" => await DispatchMinMaxAsync(query, prop, isMin: false, cancellationToken).ConfigureAwait(false),
            "avg" => await DispatchAvgAsync(query, prop, cancellationToken).ConfigureAwait(false),
            _     => throw new InvalidOperationException($"Unknown aggregate function '{function}'.")
        };

        return new { function = function.ToLowerInvariant(), field = fieldName, value = result };
    }

    PropertyInfo ResolveNumericField(string jsonName)
    {
        foreach (var f in this.Fields)
        {
            if (f.ClrProperty != null && string.Equals(f.JsonName, jsonName, StringComparison.Ordinal))
            {
                if (!IsNumeric(f.ClrType))
                    throw new InvalidOperationException(
                        $"Field '{jsonName}' is not numeric; only numeric fields are supported for sum/min/max/avg.");
                return f.ClrProperty;
            }
        }
        throw new InvalidOperationException($"Field '{jsonName}' is not allowed by the tool configuration.");
    }

    static bool IsNumeric(Type t)
    {
        var u = Nullable.GetUnderlyingType(t) ?? t;
        return u == typeof(int) || u == typeof(long) || u == typeof(short) || u == typeof(byte)
            || u == typeof(uint) || u == typeof(ulong) || u == typeof(float) || u == typeof(double)
            || u == typeof(decimal);
    }

    static async Task<object?> DispatchSumAsync(IDocumentQuery<T> query, PropertyInfo prop, CancellationToken ct)
    {
        var t = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;
        if (t == typeof(int))     return await query.Sum(BuildSelector<int>(prop), ct).ConfigureAwait(false);
        if (t == typeof(long))    return await query.Sum(BuildSelector<long>(prop), ct).ConfigureAwait(false);
        if (t == typeof(double))  return await query.Sum(BuildSelector<double>(prop), ct).ConfigureAwait(false);
        if (t == typeof(float))   return await query.Sum(BuildSelector<float>(prop), ct).ConfigureAwait(false);
        if (t == typeof(decimal)) return await query.Sum(BuildSelector<decimal>(prop), ct).ConfigureAwait(false);
        throw new InvalidOperationException($"Sum not supported for type '{t}'.");
    }

    static async Task<object?> DispatchMinMaxAsync(IDocumentQuery<T> query, PropertyInfo prop, bool isMin, CancellationToken ct)
    {
        var t = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;
        if (t == typeof(int))     return isMin ? await query.Min(BuildSelector<int>(prop), ct).ConfigureAwait(false)     : await query.Max(BuildSelector<int>(prop), ct).ConfigureAwait(false);
        if (t == typeof(long))    return isMin ? await query.Min(BuildSelector<long>(prop), ct).ConfigureAwait(false)    : await query.Max(BuildSelector<long>(prop), ct).ConfigureAwait(false);
        if (t == typeof(double))  return isMin ? await query.Min(BuildSelector<double>(prop), ct).ConfigureAwait(false)  : await query.Max(BuildSelector<double>(prop), ct).ConfigureAwait(false);
        if (t == typeof(float))   return isMin ? await query.Min(BuildSelector<float>(prop), ct).ConfigureAwait(false)   : await query.Max(BuildSelector<float>(prop), ct).ConfigureAwait(false);
        if (t == typeof(decimal)) return isMin ? await query.Min(BuildSelector<decimal>(prop), ct).ConfigureAwait(false) : await query.Max(BuildSelector<decimal>(prop), ct).ConfigureAwait(false);
        throw new InvalidOperationException($"Min/Max not supported for type '{t}'.");
    }

    static async Task<object?> DispatchAvgAsync(IDocumentQuery<T> query, PropertyInfo prop, CancellationToken ct)
    {
        var parameter = Expression.Parameter(typeof(T), "x");
        Expression body = Expression.Property(parameter, prop);
        if (body.Type.IsValueType)
            body = Expression.Convert(body, typeof(object));
        var selector = Expression.Lambda<Func<T, object>>(body, parameter);
        return await query.Average(selector, ct).ConfigureAwait(false);
    }

    static Expression<Func<T, TValue>> BuildSelector<TValue>(PropertyInfo prop)
    {
        var parameter = Expression.Parameter(typeof(T), "x");
        Expression body = Expression.Property(parameter, prop);
        if (body.Type != typeof(TValue))
            body = Expression.Convert(body, typeof(TValue));
        return Expression.Lambda<Func<T, TValue>>(body, parameter);
    }

    public static JsonElement BuildSchema(IReadOnlyList<DocumentField> fields)
    {
        var node = new JsonObject
        {
            ["type"] = "object",
            ["properties"] = new JsonObject
            {
                ["function"] = new JsonObject
                {
                    ["type"] = "string",
                    ["enum"] = new JsonArray("count", "sum", "min", "max", "avg"),
                    ["description"] = "Aggregate function. 'count' ignores 'field'; sum/min/max/avg require a numeric 'field'."
                },
                ["field"] = new JsonObject
                {
                    ["type"] = new JsonArray("string", "null"),
                    ["enum"] = BuildNullableEnum(fields),
                    ["description"] = "Numeric field to aggregate. Required for sum/min/max/avg."
                },
                ["filter"] = SchemaBuilder.BuildFilterSchema(fields)
            },
            ["required"] = new JsonArray("function")
        };
        return SchemaBuilder.ToJsonElement(node);
    }

    static JsonArray BuildNullableEnum(IReadOnlyList<DocumentField> fields)
    {
        var arr = new JsonArray();
        arr.Add((JsonNode?)null);
        foreach (var f in fields)
            arr.Add(f.JsonName);
        return arr;
    }
}
