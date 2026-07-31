namespace ShinyDocDbMyAdmin.Services;

/// <summary>
/// The public-playground switch: one setting that turns this into a instance strangers can poke at
/// without being able to damage it, reconfigure it, or use it as a proxy to somewhere else.
/// </summary>
/// <remarks>
/// <para>
/// Set <c>ShinyDocDbMyAdmin:DemoMode</c> (env: <c>ShinyDocDbMyAdmin__DemoMode=true</c>) and on
/// startup the app builds its own SQLite sample from data embedded in the image, publishes it as a
/// read-only connection, and closes everything that would let a visitor point the tool somewhere
/// else or take its configuration away with them.
/// </para>
/// <para>
/// It is deliberately one flag rather than a checklist. A playground assembled out of
/// <c>ReadOnly</c> plus <c>DisableAi</c> plus a seeding sidecar plus a read-only mount is a
/// playground that is one forgotten setting away from being an open database client on the public
/// internet.
/// </para>
/// <para>
/// What stays open is as deliberate as what closes: <b>exporting data still works</b>. Export is
/// most of what someone is at a playground to try, it produces a file rather than changing
/// anything, and the data is a published sample. Importing does not, because that writes.
/// </para>
/// </remarks>
public sealed class DemoMode
{
    public const string ConfigurationKey = "ShinyDocDbMyAdmin:DemoMode";

    /// <summary>The connection name the seeded sample is published under.</summary>
    public const string ConnectionName = "Demo";

    /// <summary>File name of the sample inside the data directory.</summary>
    public const string DatabaseFileName = "demo.db";

    public DemoMode(IConfiguration configuration, ILogger<DemoMode> logger)
    {
        this.IsEnabled = IsEnabledIn(configuration);

        if (this.IsEnabled)
            logger.LogInformation("Demo mode is on: the sample is read-only, and connection changes, settings transfer and data import are closed.");
    }

    /// <summary>
    /// Readable before the container is built, because <c>Program.cs</c> has to decide whether to
    /// seed the database and force the other settings while it is still configuring services.
    /// </summary>
    public static bool IsEnabledIn(IConfiguration configuration)
        => configuration.GetValue(ConfigurationKey, false);

    public bool IsEnabled { get; }

    // ── What the mode closes ────────────────────────────────────────────
    // Named for the capability rather than the mode, so call sites read as what they are checking
    // rather than as a chain of negations.

    /// <summary>False in demo mode: the connection list is fixed at what the image publishes.</summary>
    public bool CanManageConnections => !this.IsEnabled;

    /// <summary>False in demo mode: an export of the connection list is a map of someone's estate.</summary>
    public bool CanTransferSettings => !this.IsEnabled;

    /// <summary>False in demo mode. Importing writes, and the sample is the point of the instance.</summary>
    public bool CanImportData => !this.IsEnabled;

    /// <summary>
    /// True even in demo mode. Export is the half of Import / Export worth trying, and it only reads.
    /// </summary>
    public bool CanExportData => true;

    /// <summary>
    /// False in demo mode: rebuilding the sample is offered nowhere, so a visitor cannot wipe what
    /// everyone else came to look at. The file is written once, when it is missing, and never again.
    /// </summary>
    public bool CanRebuildDemoDatabase => false;

    /// <summary>
    /// Throws when something that should not have been reachable was reached anyway. Pages call this
    /// so a deep link produces a refusal rather than a half-rendered form.
    /// </summary>
    public void AssertCanManageConnections()
    {
        if (!this.CanManageConnections)
            throw new InvalidOperationException("This is a demo instance; its connections are fixed.");
    }

    public void AssertCanTransferSettings()
    {
        if (!this.CanTransferSettings)
            throw new InvalidOperationException("This is a demo instance; settings cannot be imported or exported.");
    }

    public void AssertCanImportData()
    {
        if (!this.CanImportData)
            throw new InvalidOperationException("This is a demo instance; the sample data is read-only. Exporting still works.");
    }
}
