using System.Reflection;
using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;
using Shiny.DocumentDb.Aspire.Hosting;

namespace Shiny.DocumentDb.Aspire.Tests;

/// <summary>
/// Model-only coverage of <c>AddDocumentDbAdminTerminal</c>: nothing here launches the tool, it asserts
/// on the resource graph the AppHost would hand to Aspire - the command, the argument order, and the
/// environment the tool reads its connections out of.
/// </summary>
#pragma warning disable ASPIRETERMINAL001 // the feature under test
public class AdminTerminalHostingTests
{
    static IDistributedApplicationBuilder CreateRunBuilder()
        => DistributedApplication.CreateBuilder();

#pragma warning disable CS0618 // model-only resolution, as elsewhere in these tests
    static async Task<string[]> Args(IResource resource)
        => await ((IResourceWithArgs)resource).GetArgumentValuesAsync(DistributedApplicationOperation.Run);

    static async Task<Dictionary<string, string>> Env(IResource resource)
        => await ((IResourceWithEnvironment)resource)
            .GetEnvironmentVariableValuesAsync(DistributedApplicationOperation.Publish);
#pragma warning restore CS0618

    /// <summary>
    /// TerminalAnnotation is internal to Aspire, so this reads it reflectively rather than not at all -
    /// the annotation is the thing that makes the resource attachable, and a silent regression there
    /// leaves a tool that cannot draw.
    /// </summary>
    static (int Columns, int Rows) TerminalSize(IResource resource)
    {
        var annotation = resource.Annotations.Single(x => x.GetType().Name == "TerminalAnnotation");
        var options = annotation.GetType().GetProperty("Options")!.GetValue(annotation)!;

        return (
            (int)options.GetType().GetProperty(nameof(TerminalOptions.Columns))!.GetValue(options)!,
            (int)options.GetType().GetProperty(nameof(TerminalOptions.Rows))!.GetValue(options)!
        );
    }

    [Fact]
    public async Task IsTheGlobalToolCommand()
    {
        var builder = CreateRunBuilder();

        var terminal = builder.AddDocumentDbAdminTerminal();

        Assert.IsType<DocumentDbAdminTerminalResource>(terminal.Resource);
        Assert.Equal("documentdb-terminal", terminal.Resource.Name);
        Assert.Equal(DocumentDbAdminTerminalResourceBuilderExtensions.ToolCommand, terminal.Resource.Command);
        Assert.Equal(builder.AppHostDirectory, terminal.Resource.WorkingDirectory);
        Assert.Empty(await Args(terminal.Resource));
    }

    [Fact]
    public async Task LocalTool_GoesThroughTheSdkSoAFreshCloneGetsIt()
    {
        var builder = CreateRunBuilder();

        var terminal = builder.AddDocumentDbAdminTerminal(tool: DocumentDbAdminTerminalTool.Local);

        Assert.Equal("dotnet", terminal.Resource.Command);
        Assert.Equal(["tool", "run", "shinydocdb"], await Args(terminal.Resource));
    }

    [Fact]
    public void CarriesATerminalAtASizeTheUiFitsIn()
    {
        var builder = CreateRunBuilder();

        var terminal = builder.AddDocumentDbAdminTerminal();

        // Without this annotation there is no pseudo-terminal, and a full-screen UI attached to a pipe
        // is not a UI. Aspire's stock 120x30 cuts the explorer pane off the document grid.
        Assert.Equal(
            (DocumentDbAdminTerminalResourceBuilderExtensions.DefaultColumns,
             DocumentDbAdminTerminalResourceBuilderExtensions.DefaultRows),
            TerminalSize(terminal.Resource)
        );
    }

    [Fact]
    public void ConfigureTerminal_OverridesTheDefaultSize()
    {
        var builder = CreateRunBuilder();

        var terminal = builder.AddDocumentDbAdminTerminal(configureTerminal: options =>
        {
            options.Columns = 220;
            options.Rows = 60;
        });

        Assert.Equal((220, 60), TerminalSize(terminal.Resource));
    }

    [Fact]
    public void IsNotSomethingThatDeploys()
    {
        var builder = CreateRunBuilder();

        var terminal = builder.AddDocumentDbAdminTerminal();

        // Terminal sessions are a dev-loop feature and a dotnet tool on a developer's machine is not a
        // deployment target - so this stays out of the manifest, unlike the container front end.
        Assert.Contains(
            terminal.Resource.Annotations.OfType<ManifestPublishingCallbackAnnotation>(),
            x => x.Callback is null
        );
    }

    [Fact]
    public async Task WithReference_HandsTheToolAConnectedStore()
    {
        var builder = CreateRunBuilder();
        var store = builder.AddSqliteDocumentStore("orders", "/data/orders.db");

        var terminal = builder.AddDocumentDbAdminTerminal().WithReference(store);

        // Exactly the pair ShinyDocDbMyAdmin's ProvidedConnections reads - the same contract the
        // container front end and the client integration read, which is why this needed no change to
        // the tool itself.
        var env = await Env(terminal.Resource);
        Assert.True(env.ContainsKey("ConnectionStrings__orders"));
        Assert.Contains("orders", env["ConnectionStrings__orders"]);
        Assert.Equal("Sqlite", env["Shiny__DocumentDb__orders__Provider"]);
    }

    [Fact]
    public async Task WithStartupProfile_OpensStraightIntoAConnection()
    {
        var builder = CreateRunBuilder();

        var terminal = builder.AddDocumentDbAdminTerminal().WithStartupProfile("orders");

        Assert.Equal(["--profile", "orders"], await Args(terminal.Resource));
    }

    [Fact]
    public async Task WithStartupProfile_TakesTheStoresName()
    {
        var builder = CreateRunBuilder();
        var store = builder.AddSqliteDocumentStore("orders", "/data/orders.db");

        var terminal = builder.AddDocumentDbAdminTerminal().WithStartupProfile(store);

        // A provided connection is named after the store resource, so naming the store is the same
        // thing as naming the profile - without the AppHost author having to know that.
        Assert.Equal(["--profile", "orders"], await Args(terminal.Resource));
    }

    [Fact]
    public async Task WithStartupProfile_IsOneArgumentHoweverOftenItIsSet()
    {
        var builder = CreateRunBuilder();

        var terminal = builder
            .AddDocumentDbAdminTerminal()
            .WithStartupProfile("orders")
            .WithStartupProfile("invoices");

        Assert.Equal(["--profile", "invoices"], await Args(terminal.Resource));
    }

    [Fact]
    public async Task LauncherArgumentsComeFirstWhateverOrderTheAppHostIsWrittenIn()
    {
        var builder = CreateRunBuilder();

        // The profile is set before the resource is even finished being described; `dotnet tool run`
        // still has to lead, or the SDK gets handed --profile as its own argument.
        var terminal = builder
            .AddDocumentDbAdminTerminal(tool: DocumentDbAdminTerminalTool.Local)
            .WithStartupProfile("orders");

        Assert.Equal(["tool", "run", "shinydocdb", "--profile", "orders"], await Args(terminal.Resource));
    }

    [Fact]
    public async Task WithDataDirectory_MovesTheToolsOwnState()
    {
        var builder = CreateRunBuilder();

        var terminal = builder.AddDocumentDbAdminTerminal().WithDataDirectory("./scratch-admin");

        // The underscore-free spelling matters: the tool resolves its settings file from this variable
        // before configuration exists, so the Shiny__ form would move half the state.
        var env = await Env(terminal.Resource);
        Assert.Equal(Path.GetFullPath("./scratch-admin"), env["SHINYDOCDBMYADMIN_DATA"]);
    }

    [Fact]
    public async Task WithReadOnly_BlocksWritesAcrossEveryStore()
    {
        var builder = CreateRunBuilder();

        var terminal = builder.AddDocumentDbAdminTerminal().WithReadOnly();

        Assert.Equal("true", (await Env(terminal.Resource))["ShinyDocDbMyAdmin__ReadOnly"]);
    }

    [Fact]
    public async Task WithReadOnly_False_IsExplicitlyWritable()
    {
        var builder = CreateRunBuilder();

        var terminal = builder.AddDocumentDbAdminTerminal().WithReadOnly(false);

        Assert.Equal("false", (await Env(terminal.Resource))["ShinyDocDbMyAdmin__ReadOnly"]);
    }

    [Fact]
    public async Task WithoutAi_TakesTheAssistantOut()
    {
        var builder = CreateRunBuilder();

        var terminal = builder.AddDocumentDbAdminTerminal().WithoutAi();

        Assert.Equal("true", (await Env(terminal.Resource))["ShinyDocDbMyAdmin__DisableAi"]);
    }

    [Fact]
    public void TheSurfaceIsMarkedExperimentalBecauseAspiresTerminalSupportIs()
    {
        // Aspire ships WithTerminal under ASPIRETERMINAL001. Carrying the same id rather than one of
        // our own means a single suppression in the AppHost covers both, and that dropping the
        // attribute here is a deliberate act once Aspire drops theirs.
        var attribute = typeof(DocumentDbAdminTerminalResourceBuilderExtensions)
            .GetCustomAttribute<System.Diagnostics.CodeAnalysis.ExperimentalAttribute>();

        Assert.NotNull(attribute);
        Assert.Equal("ASPIRETERMINAL001", attribute.DiagnosticId);
    }
}
#pragma warning restore ASPIRETERMINAL001
