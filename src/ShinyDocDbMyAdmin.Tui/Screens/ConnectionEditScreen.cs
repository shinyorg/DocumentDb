using Microsoft.Extensions.DependencyInjection;
using ShinyDocDbMyAdmin.Models;
using ShinyDocDbMyAdmin.Providers;
using ShinyDocDbMyAdmin.Services;
using ShinyDocDbMyAdmin.Tui.Shell;
using ShinyDocDbMyAdmin.Tui.Widgets;
using XenoAtom.Terminal.UI;
using XenoAtom.Terminal.UI.Controls;
using XenoAtom.Terminal.UI.Geometry;

namespace ShinyDocDbMyAdmin.Tui.Screens;

/// <summary>
/// The connection form. Same fields, same validation and same wording as the web front end's.
/// </summary>
/// <remarks>
/// The one field that has no terminal equivalent is the file upload. On the web it exists because the
/// browser cannot hand the server a path - it has to hand it bytes. A tool running on your own machine
/// already has the path, so the file-backed providers simply take one, and no copy of your database is
/// made anywhere.
/// </remarks>
public sealed class ConnectionEditScreen : Screen
{
    readonly string? profileId;
    readonly State<string> name = new("");
    readonly State<string> connectionString = new("");
    readonly State<string> password = new("");
    readonly State<bool> readOnly = new(false);
    readonly State<ProviderKind> provider = new(ProviderKind.Sqlite);
    readonly State<string> encryptionKeys = new("");
    readonly State<string> message = new("");

    ConnectionProfile profile = new();

    public ConnectionEditScreen(AdminShell shell, string? profileId) : base(shell)
    {
        this.profileId = profileId;
        this.IsNew = profileId is null;
    }

    public bool IsNew { get; private set; }

    public override string Title => this.IsNew ? "New connection" : this.profile.Name;

    ProviderDescriptor Descriptor => ProviderCatalog.Get(this.provider.Value);

    protected override Visual Build()
    {
        var providers = ProviderCatalog.Descriptors.ToList();
        var select = new Select<string>(
            [.. providers.Select(d => d.DisplayName)],
            providers.FindIndex(d => d.Kind == this.provider.Value)
        );
        select.SelectionChanged(
            () => this.OnProviderChanged(providers[Math.Clamp(select.SelectedIndex, 0, providers.Count - 1)])
        );

        var form = new VStack(
            Field("Name", Ui.Input(this.name).AutoFocus(true), "Shown in the list and the explorer tree."),
            Field("Provider", select, null),

            new ComputedVisual(() => Field(
                this.Descriptor.IsFileBased ? "File path" : "Connection string",
                Ui.Input(this.connectionString),
                this.Descriptor.IsFileBased
                    ? $"A path on this machine. {string.Join(" ", this.Descriptor.FileExtensions)}"
                    : "Stored encrypted. Set ShinyDocDbMyAdmin:SecretKey to control the key."
            )),

            new ComputedVisual(() => this.Descriptor.RequiresPassword
                ? Field(
                    "Encryption key",
                    Ui.Input(this.password).IsPassword(true),
                    "Required - SQLCipher cannot open the file without it."
                )
                : Ui.Nothing()
            ),

            new CheckBox(new TextBlock("Read-only (block every write, and refuse non-SELECT statements in the SQL console)"))
                .IsChecked(this.readOnly),

            Field(
                "Field-level encryption keys (optional)",
                Ui.Area(this.encryptionKeys).MinHeight(4).MinWidth(64),
                "One per line, as keyId=base64key - the keyId is the k1 in enc:1:k1:… . These let this tool READ " +
                "protected values; they are stored beside the connection string and are only as safe as this " +
                "installation. Leave empty and everything else still works: encrypted fields are recognised, counted " +
                "and protected from being overwritten with no key at all."
            ),

            new Markup(() => this.message.Value).Wrap(true),

            Ui.Toolbar(
                Ui.Primary("Save", () => this.Save(andTest: false)),
                Ui.Action("Save and test", () => this.Save(andTest: true)),
                Ui.Action("Cancel", this.Shell.Pop)
            )
        ).Spacing(1);

        return new Padder(new VStack(
            new Markup(() => this.IsNew ? "[bold]New connection[/]" : $"[bold]Edit '{Ui.Escape(this.profile.Name)}'[/]"),
            new ScrollViewer(form).MaxWidth(96).Stretch()
        ).Spacing(1)).Padding(new Thickness(1));
    }

    static Visual Field(string label, Visual input, string? hint)
    {
        var stack = new VStack(Ui.Label(label), input).Spacing(0);
        if (hint is not null)
            stack.Add(new Markup($"[dim]{Ui.Escape(hint)}[/]").Wrap(true));
        return stack;
    }

    public override async Task Load(CancellationToken ct)
    {
        if (this.profileId is null)
        {
            this.Shell.Post(() =>
            {
                this.profile = new ConnectionProfile();
                this.connectionString.Value = this.Descriptor.ConnectionStringTemplate;
            });
            return;
        }

        // A host-provided connection has no editable form: it is declared in the AppHost, not here.
        var existing = this.Shell.Profiles.IsProvided(this.profileId)
            ? null
            : await this.Shell.Profiles.Get(this.profileId, ct);

        if (existing is null)
        {
            this.Shell.Post(() =>
            {
                this.Shell.Warn("That connection is no longer editable here.");
                this.Shell.Pop();
            });
            return;
        }

        var revealedConnection = this.Shell.Profiles.RevealConnectionString(existing);
        var revealedPassword = this.Shell.Profiles.RevealPassword(existing) ?? "";
        var revealedKeys = string.Join(
            Environment.NewLine,
            this.Shell.Profiles.RevealEncryptionKeys(existing).Select(k => $"{k.KeyId}={k.Key}"));

        this.Shell.Post(() =>
        {
            this.profile = existing;
            this.IsNew = false;
            this.name.Value = existing.Name;
            this.provider.Value = existing.Provider;
            this.readOnly.Value = existing.ReadOnly;
            this.connectionString.Value = revealedConnection;
            this.password.Value = revealedPassword;
            this.encryptionKeys.Value = revealedKeys;
        });
    }

    /// <summary>Non-blank lines of the key box, trimmed. Blank lines and stray whitespace are not errors.</summary>
    static IEnumerable<string> KeyLines(string text)
        => text.Split(['\r', '\n'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

    void OnProviderChanged(ProviderDescriptor descriptor)
    {
        var wasTemplate = this.connectionString.Value == this.Descriptor.ConnectionStringTemplate;
        this.provider.Value = descriptor.Kind;

        // Only replace the connection string if the user has not typed their own.
        if (wasTemplate || string.IsNullOrWhiteSpace(this.connectionString.Value))
            this.connectionString.Value = descriptor.ConnectionStringTemplate;
    }

    string? Validate()
    {
        if (string.IsNullOrWhiteSpace(this.name.Value))
            return "Give the connection a name.";

        if (string.IsNullOrWhiteSpace(this.connectionString.Value))
            return this.Descriptor.IsFileBased ? "Enter a path to the database file." : "Enter a connection string.";

        if (this.Descriptor.RequiresPassword && string.IsNullOrEmpty(this.password.Value))
            return "SQLCipher connections need an encryption key.";

        foreach (var line in KeyLines(this.encryptionKeys.Value))
        {
            if (!line.Contains('='))
                return $"'{line}' is not a key. Write one per line as keyId=base64key.";
        }

        return null;
    }

    void Save(bool andTest)
    {
        if (this.Validate() is { } problem)
        {
            this.message.Value = $"[red]{Ui.Escape(problem)}[/]";
            return;
        }

        this.message.Value = "";

        var id = this.profile.Id;
        var toSave = this.profile;
        toSave.Name = this.name.Value.Trim();
        toSave.Provider = this.provider.Value;
        toSave.ReadOnly = this.readOnly.Value;
        toSave.EncryptionKeys = [.. KeyLines(this.encryptionKeys.Value).Select(line =>
        {
            var split = line.Split('=', 2);
            return new EncryptionKeyEntry { KeyId = split[0].Trim(), Key = split[1].Trim() };
        })];

        var connection = this.connectionString.Value.Trim();
        var secret = this.password.Value;

        this.Shell.RunAsync(async ct =>
        {
            await this.Shell.Profiles.Save(toSave, connection, secret, ct);

            // The provider instance is built from these values, so any cached one is now wrong.
            this.Shell.Services.GetRequiredService<ConnectionManager>().Evict(id);
            this.Shell.Admin.InvalidateSchema(id);

            if (andTest)
            {
                var result = await this.Shell.Admin.TestConnection(id, ct);
                this.Shell.Post(() =>
                {
                    this.IsNew = false;
                    this.message.Value = $"[green]{Ui.Escape(result)}[/]";
                    this.Shell.ReloadExplorer();
                });
            }
            else
            {
                this.Shell.Post(() =>
                {
                    this.Shell.Success($"Saved '{toSave.Name}'.");
                    this.Shell.ReloadExplorer();
                    this.Shell.Replace(new DatabaseOverviewScreen(this.Shell, id));
                });
            }
        }, "Could not save the connection");
    }
}
