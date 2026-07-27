namespace ShinyDocDbMyAdmin.Services;

/// <summary>
/// Circuit-scoped nudges between otherwise unrelated components - chiefly telling the explorer tree
/// that the connection list or a table's contents changed underneath it.
/// </summary>
public sealed class UiEvents
{
    /// <summary>Raised when connection profiles are added, edited or removed.</summary>
    public event Func<Task>? ProfilesChanged;

    /// <summary>Raised when documents were written, so counts in the tree are stale.</summary>
    public event Func<Task>? DataChanged;

    public Task NotifyProfilesChanged() => Invoke(this.ProfilesChanged);

    public Task NotifyDataChanged() => Invoke(this.DataChanged);

    static Task Invoke(Func<Task>? handler)
        => handler is null
            ? Task.CompletedTask
            : Task.WhenAll(handler.GetInvocationList().Cast<Func<Task>>().Select(h => h()));
}
