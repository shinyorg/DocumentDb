namespace Shiny.DocumentDb.MongoDb;

/// <summary>
/// Internal envelope document stored in MongoDB collections.
/// Mirrors the SQL Id/TypeName/Data/CreatedAt/UpdatedAt schema so behaviour stays consistent.
/// </summary>
internal static class MongoFields
{
    public const string Id = "_id";
    public const string DocId = "id";
    public const string TypeName = "typeName";
    public const string Data = "data";
    public const string CreatedAt = "createdAt";
    public const string UpdatedAt = "updatedAt";
}
