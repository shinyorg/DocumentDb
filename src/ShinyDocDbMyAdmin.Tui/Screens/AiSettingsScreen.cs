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
/// The assistant's configuration for one connection.
/// </summary>
/// <remarks>
/// Kept as a side document keyed by profile id rather than as a field on the connection: writing a
/// profile is blocked for host-provided (Aspire) connections, so settings that lived there could never
/// be given to one. The API key goes in encrypted and is only decrypted when a client is built.
/// </remarks>
public sealed class AiSettingsScreen(AdminShell shell, string profileId) : Screen(shell)
{
    readonly State<string> model = new("");
    readonly State<string> endpoint = new("");
    readonly State<string> apiKey = new("");
    readonly State<bool> enabled = new(false);
    readonly State<bool> allowInsert = new(false);
    readonly State<bool> allowUpdate = new(false);
    readonly State<bool> allowDelete = new(false);
    readonly State<AiProviderKind> provider = new(AiProviderKind.OpenAI);
    readonly State<string> message = new("");
    readonly State<string> connectionName = new("");

    AiConnectionSettings settings = new();
    Select<string>? providerSelect;

    public string ProfileId { get; } = profileId;

    public override string Title => "Assistant settings";

    AiProviderDescriptor Descriptor => AiProviderCatalog.Get(this.provider.Value);

    protected override Visual Build()
    {
        var providers = AiProviderCatalog.All;
        this.providerSelect = new Select<string>([.. providers.Select(p => p.DisplayName)]);
        this.providerSelect.SelectionChanged(() =>
        {
            var chosen = providers[Math.Clamp(this.providerSelect!.SelectedIndex, 0, providers.Count - 1)];
            this.provider.Value = chosen.Kind;

            if (string.IsNullOrWhiteSpace(this.endpoint.Value) && chosen.EndpointPlaceholder is { } placeholder)
                this.endpoint.Value = placeholder;
        });

        var form = new VStack(
            new VStack(Ui.Label("Provider"), this.providerSelect).Spacing(0),
            new ComputedVisual(() => Ui.Muted(this.Descriptor.Summary)),

            new ComputedVisual(() => new VStack(
                Ui.Label(this.Descriptor.ModelLabel),
                Ui.Input(this.model).MinWidth(48),
                Ui.Muted(this.Descriptor.SuggestedModels.Count == 0
                    ? "Free text."
                    : $"For example: {string.Join(", ", this.Descriptor.SuggestedModels)}")
            ).Spacing(0)),

            new ComputedVisual(() => this.Descriptor.RequiresEndpoint || this.Descriptor.EndpointPlaceholder is not null
                ? new VStack(
                    Ui.Label("Endpoint"),
                    Ui.Input(this.endpoint).MinWidth(56),
                    Ui.Muted(this.Descriptor.RequiresEndpoint ? "Required for this backend." : "Optional - the SDK default is used when blank.")
                ).Spacing(0)
                : Ui.Nothing()),

            new ComputedVisual(() => this.Descriptor.RequiresApiKey || this.Descriptor.KeyHelpUrl is not null
                ? new VStack(
                    Ui.Label("API key"),
                    Ui.Input(this.apiKey).IsPassword(true).MinWidth(56),
                    Ui.Muted(this.Descriptor.KeyHelpUrl is { } url
                        ? $"Stored encrypted. Keys: {url}"
                        : "Stored encrypted, under this instance's key.")
                ).Spacing(0)
                : Ui.Nothing()),

            new CheckBox(new TextBlock("Enable the assistant for this connection"), this.enabled),

            new VStack(
                Ui.Label("Write access (off by default; scoped to this connection)"),
                new CheckBox(new TextBlock("Allow insert_document"), this.allowInsert),
                new CheckBox(new TextBlock("Allow update_document"), this.allowUpdate),
                new CheckBox(new TextBlock("Allow delete_document (one id per call)"), this.allowDelete),
                Ui.Muted("A connection profile marked read-only still refuses writes regardless of these.")
            ).Spacing(0),

            new Markup(() => this.message.Value).Wrap(true),

            Ui.Toolbar(
                Ui.Primary("Save", () => this.Save(andTest: false)),
                Ui.Action("Save and test", () => this.Save(andTest: true)),
                Ui.Danger("Remove", this.Remove),
                Ui.Action("Cancel", this.Shell.Pop)
            )
        ).Spacing(1);

        return new Padder(new VStack(
            new Markup(() => $"[bold]Assistant[/] [dim]for {Ui.Escape(this.connectionName.Value)}[/]"),
            new ComputedVisual(() => Ui.Warning(this.WarningText())),
            new ScrollViewer(form).MaxWidth(96).Stretch()
        ).Spacing(1)).Padding(new Thickness(1));
    }

    public override async Task Load(CancellationToken ct)
    {
        this.Shell.AiAvailability.AssertEnabled();

        var profile = await this.Shell.Profiles.Get(this.ProfileId, ct);
        var existing = await this.Shell.Profiles.GetAiSettings(this.ProfileId, ct);
        var key = existing is null ? "" : this.Shell.Profiles.RevealApiKey(existing) ?? "";

        this.Shell.Post(() =>
        {
            this.connectionName.Value = profile?.Name ?? this.ProfileId;
            this.settings = existing ?? new AiConnectionSettings { ProfileId = this.ProfileId };

            this.provider.Value = this.settings.Provider;
            this.model.Value = this.settings.Model;
            this.endpoint.Value = this.settings.Endpoint ?? this.Descriptor.EndpointPlaceholder ?? "";
            this.apiKey.Value = key;
            this.enabled.Value = this.settings.Enabled;
            this.allowInsert.Value = this.settings.AllowInsert;
            this.allowUpdate.Value = this.settings.AllowUpdate;
            this.allowDelete.Value = this.settings.AllowDelete;

            if (this.providerSelect is { } select)
                select.SelectedIndex = Math.Max(0, AiProviderCatalog.All.ToList().FindIndex(p => p.Kind == this.settings.Provider));
        });
    }

    string WarningText()
    {
        var head =
            "Whatever the assistant reads is sent to the provider configured here, and its tools can read " +
            "every connection this tool knows about - not only this one.";

        if (!(this.allowInsert.Value || this.allowUpdate.Value || this.allowDelete.Value))
            return head + " It cannot write.";

        var parts = new List<string>();
        if (this.allowInsert.Value) parts.Add("insert");
        if (this.allowUpdate.Value) parts.Add("update");
        if (this.allowDelete.Value) parts.Add("delete");
        return $"{head} With the boxes below ticked it can also {string.Join(", ", parts)} documents on THIS connection.";
    }

    void Save(bool andTest)
    {
        if (string.IsNullOrWhiteSpace(this.model.Value))
        {
            this.message.Value = $"[red]Set the {Ui.Escape(this.Descriptor.ModelLabel.ToLowerInvariant())}.[/]";
            return;
        }

        var toSave = this.settings;
        toSave.ProfileId = this.ProfileId;
        toSave.Provider = this.provider.Value;
        toSave.Model = this.model.Value.Trim();
        toSave.Endpoint = string.IsNullOrWhiteSpace(this.endpoint.Value) ? null : this.endpoint.Value.Trim();
        toSave.Enabled = this.enabled.Value;
        toSave.AllowInsert = this.allowInsert.Value;
        toSave.AllowUpdate = this.allowUpdate.Value;
        toSave.AllowDelete = this.allowDelete.Value;

        var key = this.apiKey.Value;

        this.Shell.RunAsync(async ct =>
        {
            await this.Shell.Profiles.SaveAiSettings(toSave, key, ct);

            if (!andTest)
            {
                // Close the screen on a successful save; leaving the form open behind a green
                // "Saved." line looked to more than one person like the button had not fired at all.
                this.Shell.Post(() =>
                {
                    this.Shell.Success("Assistant settings saved.");
                    this.Shell.Pop();
                });
                return;
            }

            // Built from the saved shape rather than from a second, plaintext-carrying route, so the
            // test exercises exactly what a real conversation will.
            var reloaded = await this.Shell.Profiles.GetAiSettings(this.ProfileId, ct);
            if (reloaded is null || !reloaded.IsUsable())
            {
                this.Shell.Post(() => this.message.Value = "[yellow]Saved, but not yet usable - enable it and fill in the missing field.[/]");
                return;
            }

            var factory = this.Shell.Services.GetRequiredService<AiClientFactory>();
            using var client = factory.Create(reloaded);

            var response = await client.GetResponseAsync(
                [new Microsoft.Extensions.AI.ChatMessage(Microsoft.Extensions.AI.ChatRole.User, "Reply with the single word: ready.")],
                cancellationToken: ct);
            this.Shell.Post(() =>
            {
                this.Shell.Success($"Assistant saved. The model answered: {Ui.Cell(response.Text, 60)}");
                this.Shell.Pop();
            });
        }, "Could not save the assistant settings");
    }

    void Remove() => Modal.Confirm(
        this.Shell,
        "Remove assistant",
        "Delete the assistant configuration for this connection, including the stored API key?",
        "Remove",
        () => this.Shell.RunAsync(async ct =>
        {
            await this.Shell.Profiles.DeleteAiSettings(this.ProfileId, ct);
            this.Shell.Post(() =>
            {
                this.Shell.Success("Assistant configuration removed.");
                this.Shell.Pop();
            });
        }, "Could not remove the configuration")
    );
}
