using System.Text;
using Microsoft.Extensions.AI;
using ShinyDocDbMyAdmin.Services;

namespace ShinyDocDbMyAdmin.Tui.Panels;

/// <summary>Who said a line in the transcript.</summary>
public enum ChatSpeaker
{
    User,
    Assistant,

    /// <summary>The tool calls the assistant made, reported so the answer can be checked.</summary>
    Tools,

    /// <summary>Something went wrong. Shown in the transcript rather than as a toast that scrolls away.</summary>
    Error
}

/// <summary>One turn of the conversation.</summary>
public sealed class ChatTurn(ChatSpeaker speaker, string text)
{
    public ChatSpeaker Speaker { get; } = speaker;
    public string Text { get; set; } = text;
}

/// <summary>
/// One assistant conversation, over <see cref="IChatClient"/> directly.
/// </summary>
/// <remarks>
/// <para>
/// The terminal equivalent of the web front end's <c>AiChatSession</c>, minus the <c>ChatView</c>
/// session contract that one exists to satisfy - reactions, read receipts, typing indicators and
/// paging are a chat product's concerns, not a transcript's. What is kept is everything that decides
/// behaviour: the same <see cref="AiClientFactory"/>, the same <see cref="AiToolSurface"/>, the same
/// <see cref="AiPrompt"/>, and streaming, so the answer appears as it is written rather than after
/// however long the tool calls take.
/// </para>
/// <para>
/// Tool names are reported as they are used. An answer about someone's data is worth very little
/// without knowing which reads produced it, and the surface is read-only, so naming them costs nothing.
/// </para>
/// </remarks>
public sealed class TuiChatSession(IChatClient client, IReadOnlyList<AITool> tools, string systemPrompt) : IAsyncDisposable
{
    readonly List<ChatMessage> history = [new(ChatRole.System, systemPrompt)];

    /// <summary>Everything said so far, oldest first.</summary>
    public List<ChatTurn> Transcript { get; } = [];

    /// <summary>
    /// Sends a turn and streams the reply. <paramref name="onProgress"/> fires as text arrives, so the
    /// caller can repaint; the reply turn is mutated in place rather than replaced.
    /// </summary>
    public async Task Send(string message, Action onProgress, CancellationToken ct)
    {
        var body = message.Trim();
        if (body.Length == 0)
            return;

        this.Transcript.Add(new ChatTurn(ChatSpeaker.User, body));
        this.history.Add(new ChatMessage(ChatRole.User, body));

        // Added before the call: a reply can take many seconds once tool calls are involved, and a
        // transcript that shows nothing at all in that time reads as broken.
        var reply = new ChatTurn(ChatSpeaker.Assistant, "");
        this.Transcript.Add(reply);
        onProgress();

        var options = new ChatOptions
        {
            Tools = [.. tools],
            ToolMode = ChatToolMode.Auto
        };

        var text = new StringBuilder();
        var used = new List<string>();
        List<ChatMessage> turn = [];

        try
        {
            await foreach (var update in client.GetStreamingResponseAsync(this.history, options, ct))
            {
                foreach (var content in update.Contents)
                {
                    switch (content)
                    {
                        case TextContent chunk when chunk.Text.Length > 0:
                            text.Append(chunk.Text);
                            reply.Text = text.ToString();
                            onProgress();
                            break;

                        case FunctionCallContent call when !used.Contains(call.Name):
                            used.Add(call.Name);
                            onProgress();
                            break;
                    }
                }

                turn.Add(new ChatMessage(update.Role ?? ChatRole.Assistant, update.Contents));
            }

            // The function-invocation middleware puts the tool requests and results into the update
            // stream, so keeping the whole turn is what lets the next question follow on from them.
            this.history.AddRange(turn);

            if (used.Count > 0)
                this.Transcript.Insert(this.Transcript.Count - 1, new ChatTurn(ChatSpeaker.Tools, string.Join(", ", used)));

            if (text.Length == 0)
                reply.Text = "(the model returned nothing)";
        }
        catch (OperationCanceledException)
        {
            reply.Text = text.Length == 0 ? "(cancelled)" : text + "  (cancelled)";
        }
        catch (Exception ex)
        {
            this.Transcript.Remove(reply);
            this.Transcript.Add(new ChatTurn(ChatSpeaker.Error, ex.Message));
        }
        finally
        {
            onProgress();
        }
    }

    public ValueTask DisposeAsync()
    {
        client.Dispose();
        return ValueTask.CompletedTask;
    }
}
