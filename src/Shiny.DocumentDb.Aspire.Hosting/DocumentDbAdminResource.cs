using Aspire.Hosting.ApplicationModel;

namespace Shiny.DocumentDb.Aspire.Hosting;

/// <summary>
/// The DocumentDb Admin UI as an Aspire resource - a container running
/// <c>ghcr.io/shinyorg/shiny-docdb-myadmin</c>, the same image whether you are running the AppHost or
/// publishing it.
/// </summary>
public sealed class DocumentDbAdminResource(string name) : ContainerResource(name);
