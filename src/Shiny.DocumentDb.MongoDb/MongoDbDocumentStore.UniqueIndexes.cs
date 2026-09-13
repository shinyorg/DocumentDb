using System.Collections;
using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Text.Json;
using MongoDB.Bson;
using MongoDB.Driver;
using Shiny.DocumentDb.Internal;
using Shiny.DocumentDb.Internal.Query;

namespace Shiny.DocumentDb.MongoDb;

// Unique indexes (MapUniqueIndex). MongoDB enforces them natively: one unique index per mapping over data.<path>,
// made partial so it is scoped to the type, skips a document whose key has a null or missing part, and skips a
// document the mapping's filter rejects. Server-side writes ($set from SetProperty / ExecuteUpdate) are therefore
// checked by the engine too, and the E11000 error naming the index is translated back into UniqueConstraintException.
public partial class MongoDbDocumentStore
{
    readonly ConcurrentDictionary<string, Lazy<Task>> uniqueIndexTasks = new();

    // $ne / $not are not allowed in a partial filter, so "present and not null" is spelled as the set of every BSON
    // type a stored JSON value can take other than null.
    static BsonDocument PresentValue()
        => new("$type", new BsonArray { "string", "number", "bool", "object", "array", "date", "objectId", "binData" });

    Task EnsureUniqueIndexesAsync<T>(IMongoCollection<BsonDocument> collection) where T : class
        => this.EnsureUniqueIndexesAsync(collection, this.ResolveTypeName<T>(), this.options.Mappings.ResolveUniqueIndexes(typeof(T)));

    // Created once per collection per process, the first time a mapped type is written. Concurrent first writers await
    // the same creation, so none of them slips a write in before the index exists.
    async Task EnsureUniqueIndexesAsync(IMongoCollection<BsonDocument> collection, string typeName, IReadOnlyList<UniqueIndexMapping> indexes)
    {
        foreach (var index in indexes)
        {
            var key = collection.CollectionNamespace.FullName + ":" + index.Name;
            var creation = this.uniqueIndexTasks.GetOrAdd(key, _ => new Lazy<Task>(() => this.CreateUniqueIndexAsync(collection, typeName, index)));
            try
            {
                await creation.Value.ConfigureAwait(false);
            }
            catch
            {
                this.uniqueIndexTasks.TryRemove(new KeyValuePair<string, Lazy<Task>>(key, creation));
                throw;
            }
        }
    }

    async Task CreateUniqueIndexAsync(IMongoCollection<BsonDocument> collection, string typeName, UniqueIndexMapping index)
    {
        var keys = new BsonDocument();
        foreach (var path in index.GetJsonPaths(this.jsonOptions))
            keys[$"{MongoFields.Data}.{path}"] = 1;

        var model = new CreateIndexModel<BsonDocument>(keys, new CreateIndexOptions<BsonDocument>
        {
            Name = index.Name,
            Unique = true,
            PartialFilterExpression = this.BuildUniquePartialFilter(index, typeName)
        });

        this.Log($"MongoDB CREATE UNIQUE INDEX {index.Name} on {collection.CollectionNamespace.CollectionName}");
        try
        {
            await collection.Indexes.CreateOneAsync(model).ConfigureAwait(false);
        }
        catch (MongoException ex)
        {
            // Not swallowed: an index that failed to create stops enforcing uniqueness, and the likeliest cause —
            // documents already stored that share a key — is something only the caller can resolve.
            throw new DocumentConfigurationException(
                $"Could not create unique index '{index.Name}' for '{index.DocumentType.Name}' on collection '{collection.CollectionNamespace.CollectionName}'. " +
                "If documents already stored there share a key, resolve the duplicates first; otherwise see the inner exception.",
                ex);
        }
    }

    BsonDocument BuildUniquePartialFilter(UniqueIndexMapping index, string typeName)
    {
        var clauses = new BsonArray { new BsonDocument(MongoFields.TypeName, typeName) };
        foreach (var path in index.GetJsonPaths(this.jsonOptions))
            clauses.Add(new BsonDocument($"{MongoFields.Data}.{path}", PresentValue()));
        if (index.Filter != null)
            clauses.Add(new PartialFilterTranslator(index, this.jsonOptions).Translate());
        return PartialFilterTranslator.And(clauses.Select(c => c.AsBsonDocument).ToArray());
    }

    // The unique indexes of a stored type name — for the backup import, which only knows type names.
    IReadOnlyList<UniqueIndexMapping> UniqueIndexesFor(string typeName)
        => this.options.Mappings.UniqueIndexes
            .Where(m => TypeNameResolver.Resolve(m.DocumentType, this.options.TypeNameResolution) == typeName)
            .ToList();

    UniqueConstraintException? MatchUniqueViolation<T>(Exception ex, string typeName, string? id) where T : class
        => MatchUniqueViolation(ex, this.options.Mappings.ResolveUniqueIndexes(typeof(T)), typeName, id);

    // A duplicate-key error is a unique index only when its message names the index; anything else (the _id of a
    // real id collision) is left to the caller's own handling.
    static UniqueConstraintException? MatchUniqueViolation(Exception ex, IReadOnlyList<UniqueIndexMapping> indexes, string typeName, string? id)
    {
        if (indexes.Count == 0)
            return null;

        var messages = DuplicateKeyMessages(ex);
        foreach (var index in indexes)
        {
            if (messages.Any(m => IDatabaseProvider.MentionsIdentifier(m, index.Name)))
                return new UniqueConstraintException(typeName, index, id, ex);
        }
        return null;
    }

    static IReadOnlyList<string> DuplicateKeyMessages(Exception ex) => ex switch
    {
        MongoWriteException { WriteError.Category: ServerErrorCategory.DuplicateKey } write => [write.WriteError.Message],
        MongoBulkWriteException bulk => bulk.WriteErrors.Where(e => e.Category == ServerErrorCategory.DuplicateKey).Select(e => e.Message).ToList(),
        MongoCommandException { Code: 11000 } command => [command.Message],
        _ => []
    };

    // Lowers a unique index's filter into a partial filter expression. MongoDB accepts only a narrow operator set there
    // — equality, $gt/$gte/$lt/$lte, $exists, $type, $and and (6.0+) $or / $in — so a filter needing anything more is
    // rejected when the index is created, naming the construct, rather than silently enforced differently.
    sealed class PartialFilterTranslator(UniqueIndexMapping index, JsonSerializerOptions jsonOptions)
    {
        readonly ParameterExpression parameter = index.Filter!.Parameters[0];

        public BsonDocument Translate()
        {
            try
            {
                return this.Predicate(index.Filter!.Body);
            }
            catch (NotSupportedException ex)
            {
                throw new DocumentConfigurationException([
                    $"The filter of unique index '{index.Name}' on '{index.DocumentType.Name}' cannot be a MongoDB partial index filter: {ex.Message} " +
                    "Use equality, range comparisons, null checks, bool properties, &&, || and Contains over a constant list."
                ]);
            }
        }

        BsonDocument Predicate(Expression expression)
        {
            expression = StripConvert(expression);
            return expression switch
            {
                BinaryExpression { NodeType: ExpressionType.AndAlso } and
                    => And(this.Predicate(and.Left), this.Predicate(and.Right)),
                BinaryExpression { NodeType: ExpressionType.OrElse } or
                    => new BsonDocument("$or", new BsonArray { this.Predicate(or.Left), this.Predicate(or.Right) }),

                // !x.DeletedAt.HasValue → the field is null or missing.
                UnaryExpression { NodeType: ExpressionType.Not } not when AsHasValue(StripConvert(not.Operand)) is { } nullable && this.IsDocumentMember(nullable)
                    => new BsonDocument(this.Field(nullable), BsonNull.Value),
                // !x.IsDeleted → { field: false }; a partial filter has no $ne, and the serializer always writes the bool.
                UnaryExpression { NodeType: ExpressionType.Not } not when StripConvert(not.Operand) is MemberExpression negated && negated.Type == typeof(bool) && this.IsDocumentMember(negated)
                    => new BsonDocument(this.Field(negated), false),

                MemberExpression hasValue when AsHasValue(hasValue) is { } nullable && this.IsDocumentMember(nullable)
                    => new BsonDocument(this.Field(nullable), PresentValue()),
                MemberExpression member when member.Type == typeof(bool) && this.IsDocumentMember(member)
                    => new BsonDocument(this.Field(member), true),

                BinaryExpression binary => this.Comparison(binary),
                MethodCallExpression { Method.Name: "Contains" } call => this.Contains(call),
                _ => throw new NotSupportedException($"'{expression}' is not supported.")
            };
        }

        BsonDocument Comparison(BinaryExpression binary)
        {
            var left = StripConvert(binary.Left);
            var right = StripConvert(binary.Right);
            var memberOnLeft = this.IsDocumentMember(left);
            var (memberSide, valueSide) = memberOnLeft ? (left, right) : (right, left);
            if (!this.IsDocumentMember(memberSide) || ReferencesParameter(valueSide))
                throw new NotSupportedException($"'{binary}' must compare a document property with a constant.");

            var member = (MemberExpression)memberSide;
            var field = this.Field(member);
            var op = memberOnLeft ? binary.NodeType : Flip(binary.NodeType);
            var value = ExpressionInterpreter.EvaluateClosed(valueSide);

            if (value == null)
            {
                return op switch
                {
                    ExpressionType.Equal => new BsonDocument(field, BsonNull.Value),
                    ExpressionType.NotEqual => new BsonDocument(field, PresentValue()),
                    _ => throw new NotSupportedException($"'{binary}' orders against null.")
                };
            }

            var bson = this.ToBson(value, member.Type);
            return op switch
            {
                ExpressionType.Equal => new BsonDocument(field, bson),
                ExpressionType.NotEqual when bson.IsBoolean => new BsonDocument(field, !bson.AsBoolean),
                ExpressionType.GreaterThan => new BsonDocument(field, new BsonDocument("$gt", bson)),
                ExpressionType.GreaterThanOrEqual => new BsonDocument(field, new BsonDocument("$gte", bson)),
                ExpressionType.LessThan => new BsonDocument(field, new BsonDocument("$lt", bson)),
                ExpressionType.LessThanOrEqual => new BsonDocument(field, new BsonDocument("$lte", bson)),
                _ => throw new NotSupportedException($"'{binary}' uses {op}; only != null and != on a bool can be rewritten without $ne.")
            };
        }

        // list.Contains(x.Prop) or Enumerable.Contains(list, x.Prop) over a constant list → $in.
        BsonDocument Contains(MethodCallExpression call)
        {
            var (source, item) = call.Object != null
                ? (call.Object, call.Arguments[0])
                : (call.Arguments[0], call.Arguments.Count == 2 ? call.Arguments[1] : call.Arguments[0]);
            var member = StripConvert(item) as MemberExpression;
            var supported = call.Arguments.Count == (call.Object != null ? 1 : 2)
                && member != null
                && this.IsDocumentMember(member)
                && !ReferencesParameter(source);
            if (!supported || ExpressionInterpreter.EvaluateClosed(source) is not IEnumerable values || values is string)
                throw new NotSupportedException($"'{call}' must be Contains over a constant list with a document property.");

            var array = new BsonArray();
            foreach (var value in values)
                array.Add(value == null ? BsonNull.Value : this.ToBson(value, member!.Type));
            return new BsonDocument(this.Field(member!), new BsonDocument("$in", array));
        }

        // Serialized through the store's options so the literal matches the stored representation (enum names, date
        // formats, naming of nested objects).
        BsonValue ToBson(object value, Type memberType)
        {
            var target = Nullable.GetUnderlyingType(memberType) ?? memberType;
            if (target.IsEnum && value.GetType() != target)
                value = Enum.ToObject(target, value);
            return ConvertJsonToBson(JsonSerializer.Serialize(value, jsonOptions.GetTypeInfo(value.GetType())));
        }

        string Field(MemberExpression member)
        {
            var chain = new List<string>();
            Expression? current = member;
            while (current is MemberExpression m)
            {
                chain.Insert(0, m.Member.Name);
                current = m.Expression;
            }

            string path;
            try
            {
                path = JsonPropertyNameResolver.BuildJsonPath(jsonOptions, jsonOptions.GetTypeInfo(index.DocumentType), chain);
            }
            catch (Exception ex) when (ex is InvalidOperationException or NotSupportedException)
            {
                path = string.Join('.', chain.Select(segment => jsonOptions.PropertyNamingPolicy?.ConvertName(segment) ?? segment));
            }
            return $"{MongoFields.Data}.{path}";
        }

        bool IsDocumentMember(Expression expression)
        {
            var current = expression;
            while (current is MemberExpression m)
                current = m.Expression;
            return expression is MemberExpression && current == this.parameter;
        }

        static MemberExpression? AsHasValue(Expression expression)
            => expression is MemberExpression { Member.Name: "HasValue", Expression: MemberExpression nullable } && Nullable.GetUnderlyingType(nullable.Type) != null
                ? nullable
                : null;

        // A partial filter only accepts $and at its top level, so nested conjunctions are flattened into one.
        public static BsonDocument And(params BsonDocument[] parts)
        {
            var clauses = new BsonArray();
            foreach (var part in parts)
            {
                if (part.ElementCount == 1 && part.Contains("$and"))
                    clauses.AddRange(part["$and"].AsBsonArray);
                else
                    clauses.Add(part);
            }
            return new BsonDocument("$and", clauses);
        }

        static ExpressionType Flip(ExpressionType op) => op switch
        {
            ExpressionType.GreaterThan => ExpressionType.LessThan,
            ExpressionType.GreaterThanOrEqual => ExpressionType.LessThanOrEqual,
            ExpressionType.LessThan => ExpressionType.GreaterThan,
            ExpressionType.LessThanOrEqual => ExpressionType.GreaterThanOrEqual,
            _ => op
        };

        static Expression StripConvert(Expression expression)
        {
            while (expression is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } convert)
                expression = convert.Operand;
            return expression;
        }

        static bool ReferencesParameter(Expression expression)
        {
            var finder = new ParameterFinder();
            finder.Visit(expression);
            return finder.Found;
        }

        sealed class ParameterFinder : ExpressionVisitor
        {
            public bool Found { get; private set; }

            protected override Expression VisitParameter(ParameterExpression node)
            {
                this.Found = true;
                return node;
            }
        }
    }
}
