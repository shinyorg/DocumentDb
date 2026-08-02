using Shiny.DocumentDb;

namespace ShinyDocDbMyAdmin.Providers;

/// <summary>Everything the UI needs to know about one backend without switching on <see cref="ProviderKind"/>.</summary>
public sealed record ProviderDescriptor
{
    public required ProviderKind Kind { get; init; }
    public required string DisplayName { get; init; }

    /// <summary>Short badge text shown against a connection in the explorer tree.</summary>
    public required string Badge { get; init; }

    /// <summary>A connection string that works after filling in the obvious blanks.</summary>
    public required string ConnectionStringTemplate { get; init; }

    /// <summary>Builds the DocumentDb provider. For file-backed engines the path has already been resolved.</summary>
    public required Func<ProviderConnectionRequest, IDatabaseProvider> Factory { get; init; }

    /// <summary>True when the connection is a local file the user can upload rather than a server address.</summary>
    public bool IsFileBased { get; init; }

    /// <summary>File extensions offered by the upload control, e.g. <c>.db</c>.</summary>
    public IReadOnlyList<string> FileExtensions { get; init; } = [];

    /// <summary>True when the engine needs a password separate from the connection string (SQLCipher).</summary>
    public bool RequiresPassword { get; init; }

    /// <summary>
    /// True when the engine holds an exclusive lock on its file, so the admin connection must be
    /// opened and closed around each operation rather than held open.
    /// </summary>
    public bool ExclusiveFileLock { get; init; }

    public string Placeholder => this.IsFileBased ? "/path/to/database.db" : this.ConnectionStringTemplate;
}

/// <summary>The resolved inputs handed to a provider factory.</summary>
public sealed record ProviderConnectionRequest(string ConnectionString, string? Password, string? FilePath);
