using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.MongoDb;

/// <summary>
/// A join on MongoDB: one aggregation over the left type's collection. The left document's type, filters and
/// left-only conditions are a leading <c>$match</c>; the right document comes from a correlated <c>$lookup</c>
/// sub-pipeline that matches its type, its filters and the join condition, then <c>$unwind</c> (keeping unmatched rows
/// for a left join); conditions on the right document, or across both, follow as a second <c>$match</c>.
/// </summary>
/// <remarks>
/// A condition that compares the two documents must be a comparison between two properties — what a sub-pipeline's
/// <c>$expr</c> can say with the left value bound through <c>let</c>. A left join's condition cannot test the left
/// document alone: it would have to keep the row while refusing the match, which <c>$lookup</c> cannot express.
/// </remarks>
sealed class MongoJoinQuery<TLeft, TRight> : JoinQueryBase<TLeft, TRight> where TLeft : class where TRight : class
{
    const string Joined = "__join";

    readonly MongoDbDocumentStore store;

    internal MongoJoinQuery(MongoDbDocumentStore store, JoinDefinition<TLeft, TRight> definition) : base(definition)
        => this.store = store;

    MongoJoinQuery(MongoJoinQuery<TLeft, TRight> source) : base(source)
        => this.store = source.store;

    protected override JoinQueryBase<TLeft, TRight> Clone() => new MongoJoinQuery<TLeft, TRight>(this);

    string TrackedName => $"{typeof(TLeft).Name}+{typeof(TRight).Name}";

    List<BsonDocument> BuildPipeline(int? maxRows, bool count)
    {
        var d = this.Definition;
        var prepared = this.Prepare();
        var rightData = $"{Joined}.{MongoFields.Data}";

        var leftMatch = new List<BsonDocument> { new(MongoFields.TypeName, this.store.ResolveTypeNameFor<TLeft>()) };
        leftMatch.AddRange(prepared.LeftScope.Select(e => this.Filter(e, d.LeftSource.TypeInfo, MongoFields.Data)));

        var rightMatch = new List<BsonDocument> { new(MongoFields.TypeName, this.store.ResolveTypeNameFor<TRight>()) };
        rightMatch.AddRange(prepared.RightScope.Select(e => this.Filter(e, d.RightSource.TypeInfo, MongoFields.Data)));

        var let = new BsonDocument();
        var correlated = new BsonArray();
        foreach (var condition in prepared.On)
        {
            switch (this.Uses(condition))
            {
                case JoinSideUse.Both:
                    correlated.Add(this.Comparison(condition, left => BindLeft(let, left), right => "$" + right));
                    break;

                case JoinSideUse.Left when d.Kind == JoinKind.Inner:
                    // For an inner join a condition on the left document alone filters the same rows either way.
                    leftMatch.Add(this.Filter(condition, d.LeftSource.TypeInfo, MongoFields.Data));
                    break;

                case JoinSideUse.Left:
                    throw new NotSupportedException(
                        "On MongoDB a left join's condition cannot test the left document alone — it would have to keep the row while " +
                        "refusing the match. Move that condition to Where, or use an inner join.");

                default:
                    rightMatch.Add(this.Filter(condition, d.RightSource.TypeInfo, MongoFields.Data));
                    break;
            }
        }

        var after = new List<BsonDocument>();
        foreach (var condition in prepared.Where)
        {
            switch (this.Uses(condition))
            {
                case JoinSideUse.Right:
                    after.Add(this.RightMissing(condition) ?? this.Filter(condition, d.RightSource.TypeInfo, rightData));
                    break;

                case JoinSideUse.Both:
                    after.Add(new BsonDocument("$expr", this.Comparison(condition, left => "$" + left, right => $"${Joined}.{right}")));
                    break;

                default:
                    leftMatch.Add(this.Filter(condition, d.LeftSource.TypeInfo, MongoFields.Data));
                    break;
            }
        }

        var lookupStages = new BsonArray { new BsonDocument("$match", And(rightMatch)) };
        if (correlated.Count > 0)
            lookupStages.Add(new BsonDocument("$match", new BsonDocument("$expr", correlated.Count == 1 ? correlated[0] : new BsonDocument("$and", correlated))));

        var pipeline = new List<BsonDocument>
        {
            new("$match", And(leftMatch)),
            new("$lookup", new BsonDocument
            {
                { "from", this.store.ResolveCollectionNameFor<TRight>() },
                { "let", let },
                { "pipeline", lookupStages },
                { "as", Joined }
            }),
            new("$unwind", new BsonDocument
            {
                { "path", "$" + Joined },
                { "preserveNullAndEmptyArrays", d.Kind == JoinKind.Left }
            })
        };
        if (after.Count > 0)
            pipeline.Add(new BsonDocument("$match", And(after)));

        if (count)
        {
            pipeline.Add(new BsonDocument("$count", "count"));
            return pipeline;
        }

        if (prepared.OrderBy.Count > 0)
        {
            var sort = new BsonDocument();
            foreach (var (body, descending) in prepared.OrderBy)
                sort[this.SortField(body)] = descending ? -1 : 1;
            pipeline.Add(new BsonDocument("$sort", sort));
        }

        var (skip, take) = this.Window(maxRows);
        if (skip is > 0)
            pipeline.Add(new BsonDocument("$skip", skip.Value));
        if (take is { } limit)
            pipeline.Add(new BsonDocument("$limit", limit));

        return pipeline;
    }

    // Binds a left-document field into the sub-pipeline's let, returning the variable reference.
    static string BindLeft(BsonDocument let, string leftPath)
    {
        var name = $"j{let.ElementCount}";
        let[name] = "$" + leftPath;
        return "$$" + name;
    }

    static BsonDocument And(List<BsonDocument> conditions)
        => conditions.Count == 1 ? conditions[0] : new BsonDocument("$and", new BsonArray(conditions));

    BsonDocument Filter(Expression condition, System.Text.Json.Serialization.Metadata.JsonTypeInfo typeInfo, string fieldPrefix)
    {
        var registry = BsonSerializer.SerializerRegistry;
        var args = new RenderArgs<BsonDocument>(registry.GetSerializer<BsonDocument>(), registry);
        return MongoExpressionVisitor.TranslateBody(condition, this.store.JsonOptions, typeInfo, fieldPrefix).Render(args);
    }

    // `c == null` / `c != null` over the right document: whether the lookup found one.
    BsonDocument? RightMissing(Expression condition)
    {
        if (condition is not BinaryExpression { NodeType: ExpressionType.Equal or ExpressionType.NotEqual } binary)
            return null;

        var (parameter, other) = Strip(binary.Left) is ParameterExpression p
            ? (p, binary.Right)
            : (Strip(binary.Right) as ParameterExpression, binary.Left);

        return parameter == this.Definition.Right && Strip(other) is ConstantExpression { Value: null }
            ? new BsonDocument(Joined, new BsonDocument("$exists", binary.NodeType == ExpressionType.NotEqual))
            : null;
    }

    BsonDocument Comparison(Expression condition, Func<string, string> leftReference, Func<string, string> rightReference)
    {
        var d = this.Definition;
        if (condition is not BinaryExpression binary || Operator(binary.NodeType) is not { } op)
            throw UnsupportedComparison();

        return new BsonDocument(op, new BsonArray { Operand(binary.Left), Operand(binary.Right) });

        BsonValue Operand(Expression operand)
        {
            var member = Strip(operand);
            if (member is MemberExpression && Root(member) is { } root)
            {
                if (root == d.Left)
                    return leftReference(MongoExpressionVisitor.ResolveField(member, this.store.JsonOptions, d.LeftSource.TypeInfo, MongoFields.Data));
                if (root == d.Right)
                    return rightReference(MongoExpressionVisitor.ResolveField(member, this.store.JsonOptions, d.RightSource.TypeInfo, MongoFields.Data));
            }
            throw UnsupportedComparison();
        }
    }

    string SortField(Expression body)
    {
        var d = this.Definition;
        var member = Strip(body);
        if (member is MemberExpression && Root(member) is { } root)
        {
            if (root == d.Left)
                return MongoExpressionVisitor.ResolveField(member, this.store.JsonOptions, d.LeftSource.TypeInfo, MongoFields.Data);
            if (root == d.Right)
                return MongoExpressionVisitor.ResolveField(member, this.store.JsonOptions, d.RightSource.TypeInfo, $"{Joined}.{MongoFields.Data}");
        }
        throw new NotSupportedException("On MongoDB a join can only be ordered by a property of either document.");
    }

    static string? Operator(ExpressionType type) => type switch
    {
        ExpressionType.Equal => "$eq",
        ExpressionType.NotEqual => "$ne",
        ExpressionType.GreaterThan => "$gt",
        ExpressionType.GreaterThanOrEqual => "$gte",
        ExpressionType.LessThan => "$lt",
        ExpressionType.LessThanOrEqual => "$lte",
        _ => null
    };

    static NotSupportedException UnsupportedComparison() => new(
        "On MongoDB a condition that compares the two documents must be a comparison between two properties, " +
        "e.g. (o, c) => o.CustomerId == c.Id or o.Total > c.CreditLimit.");

    static Expression Strip(Expression expression)
    {
        while (expression is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } unary)
            expression = unary.Operand;
        return expression;
    }

    static ParameterExpression? Root(Expression expression)
    {
        Expression? current = expression;
        while (current is MemberExpression member)
            current = member.Expression;
        return current as ParameterExpression;
    }

    // ── Terminals ───────────────────────────────────────────────────────

    Task<List<BsonDocument>> AggregateAsync(List<BsonDocument> pipeline, CancellationToken ct)
        => this.store.GetCollection<TLeft>()
            .Aggregate(PipelineDefinition<BsonDocument, BsonDocument>.Create(pipeline))
            .ToListAsync(ct);

    internal override Task<IReadOnlyList<JoinPair<TLeft, TRight>>> FetchAsync(int? maxRows, CancellationToken ct)
        => this.store.Tracker.Track("query.join.to_list", this.TrackedName, async () =>
        {
            var rows = await this.AggregateAsync(this.BuildPipeline(maxRows, false), ct).ConfigureAwait(false);
            IReadOnlyList<JoinPair<TLeft, TRight>> pairs = [.. rows.Select(this.ToPair)];
            return pairs;
        }, r => r.Count);

    internal override IAsyncEnumerable<JoinPair<TLeft, TRight>> StreamAsync(CancellationToken ct)
        => this.store.Tracker.TrackStream("query.join.stream", this.TrackedName, this.StreamImpl(ct), ct);

    async IAsyncEnumerable<JoinPair<TLeft, TRight>> StreamImpl([EnumeratorCancellation] CancellationToken ct)
    {
        using var cursor = await this.store.GetCollection<TLeft>()
            .AggregateAsync(PipelineDefinition<BsonDocument, BsonDocument>.Create(this.BuildPipeline(null, false)), cancellationToken: ct)
            .ConfigureAwait(false);

        while (await cursor.MoveNextAsync(ct).ConfigureAwait(false))
        {
            foreach (var row in cursor.Current)
                yield return this.ToPair(row);
        }
    }

    internal override Task<long> CountAsync(CancellationToken ct)
        => this.store.Tracker.Track("query.join.count", this.TrackedName, async () =>
        {
            var rows = await this.AggregateAsync(this.BuildPipeline(null, true), ct).ConfigureAwait(false);
            return rows.Count == 0 ? 0L : rows[0]["count"].ToInt64();
        }, r => r);

    internal override async Task<bool> AnyAsync(CancellationToken ct)
        => await this.CountAsync(ct).ConfigureAwait(false) > 0;

    internal override DocumentQueryString RenderQuery()
    {
        var command = new BsonDocument
        {
            { "aggregate", this.store.ResolveCollectionNameFor<TLeft>() },
            { "pipeline", new BsonArray(this.BuildPipeline(null, false)) }
        };
        var json = command.ToJson(new JsonWriterSettings { OutputMode = JsonOutputMode.RelaxedExtendedJson });
        return new DocumentQueryString(json, new Dictionary<string, object?>());
    }

    JoinPair<TLeft, TRight> ToPair(BsonDocument row)
    {
        var d = this.Definition;
        var left = this.store.Materialize(row, d.LeftSource.TypeInfo)!;
        ComputedReadBack.Apply([left], this.store.Options.ResolveComputedMappings(typeof(TLeft)));

        TRight? right = null;
        if (row.TryGetValue(Joined, out var joined) && joined.IsBsonDocument)
        {
            right = this.store.Materialize(joined.AsBsonDocument, d.RightSource.TypeInfo);
            if (right != null)
                ComputedReadBack.Apply([right], this.store.Options.ResolveComputedMappings(typeof(TRight)));
        }

        return new JoinPair<TLeft, TRight>(left, right);
    }
}
