namespace Sample.Aspire.Domain;

public class Category
{
    public string Id { get; set; } = null!;
    public string Name { get; set; } = null!;
}

public class Product
{
    public string Id { get; set; } = null!;
    public string Name { get; set; } = null!;
    public string CategoryId { get; set; } = null!;
    public decimal Price { get; set; }
    public int Stock { get; set; }
}
