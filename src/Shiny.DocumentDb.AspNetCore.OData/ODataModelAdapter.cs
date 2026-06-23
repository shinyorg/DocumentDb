using Microsoft.AspNetCore.OData.Query;
using Microsoft.OData.UriParser;
using Shiny.DocumentDb.OData;

namespace Shiny.DocumentDb.AspNetCore.OData;

/// <summary>
/// Converts the Microsoft-parsed <see cref="ODataQueryOptions{T}"/> (full <c>$metadata</c>/EDM grammar
/// compliance) into the engine's provider-neutral <see cref="ODataQueryModel"/>. This is the second
/// front-end onto the single <see cref="FilterExpressionBuilder"/> — the same node tree the engine's
/// lightweight string parser produces.
/// </summary>
public static class ODataModelAdapter
{
    public static ODataQueryModel ToModel<T>(ODataQueryOptions<T> options) where T : class
    {
        ArgumentNullException.ThrowIfNull(options);

        var model = new ODataQueryModel
        {
            Top = options.Top?.Value,
            Skip = options.Skip?.Value,
            Count = options.Count?.Value ?? false,
            HasExpand = HasExpand(options),
            Select = ExtractSelect(options)
        };

        if (options.Filter?.FilterClause is { } filter)
            model.Filter = ConvertFilter(filter.Expression);

        if (options.OrderBy?.OrderByClause is { } orderBy)
            model.OrderBy = ConvertOrderBy(orderBy);

        return model;
    }

    static bool HasExpand<T>(ODataQueryOptions<T> options) where T : class
    {
        var expanded = options.SelectExpand?.SelectExpandClause?.SelectedItems;
        return expanded != null && expanded.OfType<ExpandedNavigationSelectItem>().Any();
    }

    static string? ExtractSelect<T>(ODataQueryOptions<T> options) where T : class
    {
        var clause = options.SelectExpand?.SelectExpandClause;
        if (clause == null || clause.AllSelected)
            return null;

        var paths = new List<string>();
        foreach (var item in clause.SelectedItems.OfType<PathSelectItem>())
        {
            var segment = item.SelectedPath.LastSegment;
            if (segment is PropertySegment prop)
                paths.Add(prop.Property.Name);
            else if (segment != null)
                paths.Add(segment.Identifier);
        }
        return paths.Count == 0 ? null : string.Join(",", paths);
    }

    static IReadOnlyList<ODataOrderByItem> ConvertOrderBy(OrderByClause clause)
    {
        var items = new List<ODataOrderByItem>();
        var current = clause;
        while (current != null)
        {
            var path = ConvertPath(current.Expression);
            var descending = current.Direction == OrderByDirection.Descending;
            items.Add(new ODataOrderByItem(path, descending));
            current = current.ThenBy;
        }
        return items;
    }

    static string ConvertPath(SingleValueNode node) => node switch
    {
        ConvertNode convert => ConvertPath(convert.Source),
        SingleValuePropertyAccessNode prop => CombinePath(prop.Source, prop.Property.Name),
        SingleValueOpenPropertyAccessNode open => open.Name,
        _ => throw new ODataNotSupportedException($"Unsupported $orderby expression node '{node.Kind}'.")
    };

    static string CombinePath(SingleValueNode? source, string name)
    {
        var prefix = source switch
        {
            SingleValuePropertyAccessNode prop => CombinePath(prop.Source, prop.Property.Name),
            ConvertNode convert when convert.Source is SingleValuePropertyAccessNode p => CombinePath(p.Source, p.Property.Name),
            _ => null
        };
        return prefix == null ? name : prefix + "/" + name;
    }

    static ODataFilterNode ConvertFilter(SingleValueNode node)
    {
        switch (node)
        {
            case ConvertNode convert:
                return ConvertFilter(convert.Source);

            case BinaryOperatorNode binary:
                return ConvertBinary(binary);

            case UnaryOperatorNode unary when unary.OperatorKind == UnaryOperatorKind.Not:
                return ODataFilterNode.Unary(ODataUnaryOperator.Not, ConvertFilter(unary.Operand));

            case UnaryOperatorNode unary when unary.OperatorKind == UnaryOperatorKind.Negate:
                return ODataFilterNode.Unary(ODataUnaryOperator.Negate, ConvertFilter(unary.Operand));

            case SingleValuePropertyAccessNode prop:
                return ODataFilterNode.Property(CombinePath(prop.Source, prop.Property.Name));

            case SingleValueOpenPropertyAccessNode open:
                return ODataFilterNode.Property(open.Name);

            case ConstantNode constant:
                return ODataFilterNode.Constant(NormalizeConstant(constant.Value));

            case SingleValueFunctionCallNode fn:
                return ODataFilterNode.Function(fn.Name, fn.Parameters.OfType<SingleValueNode>().Select(ConvertFilter).ToArray());

            default:
                throw new ODataNotSupportedException($"Unsupported $filter node '{node.Kind}'.");
        }
    }

    static ODataFilterNode ConvertBinary(BinaryOperatorNode binary)
    {
        var op = binary.OperatorKind switch
        {
            BinaryOperatorKind.Equal => ODataBinaryOperator.Equal,
            BinaryOperatorKind.NotEqual => ODataBinaryOperator.NotEqual,
            BinaryOperatorKind.GreaterThan => ODataBinaryOperator.GreaterThan,
            BinaryOperatorKind.GreaterThanOrEqual => ODataBinaryOperator.GreaterThanOrEqual,
            BinaryOperatorKind.LessThan => ODataBinaryOperator.LessThan,
            BinaryOperatorKind.LessThanOrEqual => ODataBinaryOperator.LessThanOrEqual,
            BinaryOperatorKind.And => ODataBinaryOperator.And,
            BinaryOperatorKind.Or => ODataBinaryOperator.Or,
            BinaryOperatorKind.Add => ODataBinaryOperator.Add,
            BinaryOperatorKind.Subtract => ODataBinaryOperator.Subtract,
            BinaryOperatorKind.Multiply => ODataBinaryOperator.Multiply,
            BinaryOperatorKind.Divide => ODataBinaryOperator.Divide,
            BinaryOperatorKind.Modulo => ODataBinaryOperator.Modulo,
            _ => throw new ODataNotSupportedException($"Unsupported binary operator '{binary.OperatorKind}'.")
        };
        return ODataFilterNode.Binary(op, ConvertFilter(binary.Left), ConvertFilter(binary.Right));
    }

    static object? NormalizeConstant(object? value) => value switch
    {
        Microsoft.OData.Edm.Date d => d.ToString(),
        Microsoft.OData.Edm.TimeOfDay t => t.ToString(),
        _ => value
    };
}
