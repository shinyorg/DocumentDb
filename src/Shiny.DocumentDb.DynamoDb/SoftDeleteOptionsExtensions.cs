using System.Linq.Expressions;

namespace Shiny.DocumentDb.DynamoDb;

/// <summary>Enables soft delete on <see cref="DynamoDbDocumentStoreOptions"/> — see <see cref="SoftDelete"/>.</summary>
public static class DynamoDbSoftDeleteOptionsExtensions
{
    /// <summary>
    /// Maps a soft-delete flag on <typeparamref name="T"/>: every <c>Remove</c> / <c>ExecuteDelete</c> /
    /// <c>Clear</c> of the type sets <paramref name="flagProperty"/> instead of deleting the document, and a named
    /// query filter (<see cref="SoftDelete.FilterName"/>) hides flagged documents from every read. Pass a
    /// <c>bool</c> property (set to <c>true</c>) or a nullable <c>DateTime</c>/<c>DateTimeOffset</c> (stamped with
    /// now). Read past it with <c>Query&lt;T&gt;().IncludeDeleted()</c>; see <see cref="SoftDeleteExtensions"/>
    /// for <c>Restore</c> / <c>PurgeDeleted</c> / <c>HardDelete</c>.
    /// </summary>
    public static DynamoDbDocumentStoreOptions AddSoftDelete<T>(this DynamoDbDocumentStoreOptions options, Expression<Func<T, object>> flagProperty) where T : class
    {
        ArgumentNullException.ThrowIfNull(options);
        SoftDelete.Configure<T>(
            flagProperty,
            i => options.AddInterceptor(i),
            i => options.AddBulkInterceptor(i),
            (name, predicate) => options.AddQueryFilter<T>(name, predicate));
        return options;
    }
}
