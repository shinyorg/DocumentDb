using Microsoft.Extensions.AI;

namespace Shiny.DocumentDb.Extensions.AI;

/// <summary>
/// Bundle of <see cref="AITool"/> instances generated for the document types
/// registered via <c>AddDocumentStoreAITools</c>. Resolve from DI and pass
/// <see cref="Tools"/> to your <c>IChatClient</c> / <c>ChatOptions.Tools</c>.
/// </summary>
public sealed class DocumentStoreAITools
{
    public IReadOnlyList<AITool> Tools { get; }

    internal DocumentStoreAITools(IReadOnlyList<AITool> tools)
    {
        this.Tools = tools;
    }
}
