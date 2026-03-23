namespace Shiny.DocumentDb;

public record DocumentRecord(
    string Id,
    string TypeName,
    string Data,
    DateTimeOffset CreatedAt,
    DateTimeOffset UpdatedAt
);
