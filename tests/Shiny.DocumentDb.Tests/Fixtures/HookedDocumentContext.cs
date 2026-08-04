namespace Shiny.DocumentDb.Tests.Fixtures;

// Exercises the generated OnConfiguring hook: a context that configures its own document model next to its
// [Document] list instead of inside AddDocumentStore. The generated ConfigureModel calls this last, so what it
// sets wins over the attribute-derived mapping.
[Document(typeof(User), JsonContext = typeof(TestJsonContext))]
public partial class HookedDocumentContext : DocumentContext
{
    /// <summary>The table name the hook assigns — asserted by the tests.</summary>
    public const string UserTable = "hooked_users";

    static partial void OnConfiguring(DocumentModelBuilder model)
        => model.Document<User>(cfg =>
        {
            cfg.Table = UserTable;
            cfg.AddQueryFilter("adults", u => u.Age >= 18);
        });
}
