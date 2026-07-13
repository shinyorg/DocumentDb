namespace Shiny.DocumentDb.Aspire.Tests.Fixtures;

// Reflection-mode models (the Aspire test project keeps UseReflectionFallback on) so a real generated
// DocumentContext + DocumentSet<T> can be exercised against the AddDocumentContextProvider helper without
// hand-writing a JsonSerializerContext.
public class OrderRecord
{
    public string Id { get; set; } = "";
    public string Customer { get; set; } = "";
}

public class InvoiceRecord
{
    public string Id { get; set; } = "";
    public decimal Total { get; set; }
}

[Document(typeof(OrderRecord))]
public partial class OrdersContext : DocumentContext;

[Document(typeof(InvoiceRecord))]
public partial class InvoicesContext : DocumentContext;
