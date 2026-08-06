namespace Shiny.DocumentDb;

/// <summary>
/// Every problem the configuration validation pass found, reported together when the store is built. One
/// exception listing all of them beats fixing them one restart at a time.
/// </summary>
public sealed class DocumentConfigurationException : InvalidOperationException
{
    public DocumentConfigurationException(IReadOnlyList<string> errors)
        : base(Format(errors)) => this.Errors = errors;

    /// <summary>
    /// A single misconfiguration surfaced outside the validation pass — one the store only discovers when
    /// the backend rejects it (e.g. a missing spatial extension), so the driver error is kept as the inner.
    /// </summary>
    public DocumentConfigurationException(string error, Exception innerException)
        : base(Format([error]), innerException) => this.Errors = [error];

    /// <summary>The individual problems, in the order they were found.</summary>
    public IReadOnlyList<string> Errors { get; }

    static string Format(IReadOnlyList<string> errors)
    {
        var noun = errors.Count == 1 ? "problem" : "problems";
        return $"The document store configuration has {errors.Count} {noun}:{Environment.NewLine}" +
               string.Join(Environment.NewLine, errors.Select(e => "  • " + e));
    }
}
