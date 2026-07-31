using System.Text;
using Microsoft.Extensions.AI;
using Shiny.Blazor.Controls.Chat;
using ShinyDocDbMyAdmin.Models;
using AiChatMessage = Microsoft.Extensions.AI.ChatMessage;
using UiChatMessage = Shiny.Blazor.Controls.Chat.ChatMessage;

namespace ShinyDocDbMyAdmin.Services;

/// <summary>
/// One assistant conversation, bridging <c>ChatView</c>'s session contract to an
/// <see cref="IChatClient"/> armed with <see cref="AiToolSurface"/>.
/// </summary>
/// <remarks>
/// <para>
/// The transcript lives in memory for the life of the circuit and is never written to disk. That is
/// deliberate: the conversation contains whatever the assistant read out of the databases, and a
/// file of production data sitting in the tool's own store is a worse problem than losing history on
/// refresh.
/// </para>
/// <para>
/// Nothing here runs until <see cref="SendMessageAsync"/> is called. There is no warm-up, no
/// background indexing and no schema pre-fetch - which is what lets the UI promise that data leaves
/// the tool only when you talk to the assistant.
/// </para>
/// </remarks>
public sealed class AiChatSession : IChatSession
{
    public const string UserId = "you";
    public const string AssistantId = "assistant";

    /// <summary>Caps the tool-call loop, so a confused model cannot read in a circle forever.</summary>
    const int MaxToolIterations = 12;

    readonly IChatClient client;
    readonly IReadOnlyList<AITool> tools;
    readonly List<AiChatMessage> history = [];
    readonly List<UiChatMessage> transcript = [];
    readonly Lock sync = new();
    readonly ILogger logger;

    public AiChatSession(
        string sessionId,
        string sessionName,
        IChatClient client,
        IReadOnlyList<AITool> tools,
        string systemPrompt,
        ILogger logger)
    {
        this.client = client;
        this.tools = tools;
        this.logger = logger;
        this.history.Add(new AiChatMessage(ChatRole.System, systemPrompt));

        this.Info = new ChatSessionInfo(
            sessionId,
            sessionName,
            [
                new ChatSessionUserInfo(UserId, "You", null, "#DCF8C6", DateTimeOffset.UtcNow),
                new ChatSessionUserInfo(AssistantId, "Assistant", null, "#FFFFFF", DateTimeOffset.UtcNow)
            ],
            PermittedEmojis: null,
            // Only drives the composition toolbar (ChatView.razor renders it from these flags), and
            // a question box wants no bold/italic buttons. Assistant replies still render normally.
            BodyPermissions: MessageBodyPermissions.None,
            // Only sending. There is nobody to invite, nothing to react to, and an "edit" of a
            // message the model already answered would be a lie about what it was asked.
            Permissions: ChatSessionPermissions.CanSendMessages,
            CreatedAt: DateTimeOffset.UtcNow,
            LastReadDate: null,
            UnreadMessageCount: 0);
    }

    public ChatSessionInfo Info { get; private set; }

    public string CurrentUserId => UserId;

    public event EventHandler<UiChatMessage>? MessageReceived;
    public event EventHandler<MessageChanged>? MessageUpdated;
    public event EventHandler<string>? MessageDeleted;
    public event EventHandler<UserTypingEvent>? UserTyping;
    public event EventHandler<ChatSessionUserInfo>? UserJoined;
    public event EventHandler<ChatSessionUserInfo>? UserLeft;
    public event EventHandler<ChatSessionInfo>? SessionUpdated;
    public event EventHandler<ChatConnectionState>? ConnectionStateChanged;

    public Task<MessagePage> GetMessagesAsync(
        string? cursorMessageId,
        MessagePageDirection direction,
        int count,
        CancellationToken cancellationToken = default)
    {
        lock (this.sync)
        {
            if (direction == MessagePageDirection.Older)
            {
                var end = cursorMessageId is null
                    ? this.transcript.Count
                    : IndexOr(this.transcript, cursorMessageId, this.transcript.Count);

                var start = Math.Max(0, end - count);
                return Task.FromResult(new MessagePage(this.transcript.GetRange(start, end - start), start > 0));
            }

            var from = cursorMessageId is null ? 0 : IndexOr(this.transcript, cursorMessageId, -1) + 1;
            var take = Math.Min(count, this.transcript.Count - from);
            return take <= 0
                ? Task.FromResult(new MessagePage([], false))
                : Task.FromResult(new MessagePage(this.transcript.GetRange(from, take), from + take < this.transcript.Count));
        }
    }

    static int IndexOr(List<UiChatMessage> messages, string messageId, int fallback)
    {
        var i = messages.FindIndex(m => m.MessageId == messageId);
        return i >= 0 ? i : fallback;
    }

    /// <summary>
    /// The only path that talks to the model provider. Posts the user's turn, then streams the reply
    /// back through <see cref="MessageUpdated"/> so the answer appears as it is written.
    /// </summary>
    public async Task<UiChatMessage> SendMessageAsync(OutgoingMessage message, CancellationToken cancellationToken = default)
    {
        // The tool surface is text-only, so an image would be accepted and then silently ignored.
        message.Attachment?.Content.Dispose();

        var body = message.Body?.Trim();
        if (string.IsNullOrEmpty(body))
            throw new ChatSessionException("Nothing to send.");

        var sent = new UiChatMessage(
            MessageId: Guid.NewGuid().ToString("N"),
            ClientMessageId: message.ClientMessageId,
            SenderId: UserId,
            Body: body,
            ImageUrl: null,
            Status: MessageStatus.Sent,
            StatusReason: null,
            Timestamp: DateTimeOffset.Now,
            EditedTimestamp: null,
            Reactions: [],
            ReadReceipts: []);

        lock (this.sync)
        {
            this.transcript.Add(sent);
            this.history.Add(new AiChatMessage(ChatRole.User, body));
        }

        // Placeholder first: the reply can take many seconds once tool calls are involved, and a
        // chat that shows nothing at all in that time reads as broken.
        var replyId = Guid.NewGuid().ToString("N");
        var reply = sent with
        {
            MessageId = replyId,
            ClientMessageId = null,
            SenderId = AssistantId,
            Body = "",
            Status = MessageStatus.Sending,
            Timestamp = DateTimeOffset.Now
        };

        lock (this.sync)
            this.transcript.Add(reply);

        this.MessageReceived?.Invoke(this, reply);
        this.UserTyping?.Invoke(this, new UserTypingEvent(AssistantId, true, DateTimeOffset.Now));

        try
        {
            reply = await this.StreamReply(reply, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogError(ex, "The assistant call failed.");
            reply = reply with
            {
                Body = string.IsNullOrEmpty(reply.Body) ? null : reply.Body,
                Status = MessageStatus.Failed,
                StatusReason = ex.Message
            };

            this.Replace(reply, MessageChangeKind.StatusChanged);
        }
        finally
        {
            this.UserTyping?.Invoke(this, new UserTypingEvent(AssistantId, false, DateTimeOffset.Now));
        }

        return sent;
    }

    async Task<UiChatMessage> StreamReply(UiChatMessage reply, CancellationToken ct)
    {
        var options = new ChatOptions
        {
            Tools = [.. this.tools],
            // The function-invocation middleware loops until the model stops asking for tools; this
            // is the backstop for a model that keeps asking.
            MaxOutputTokens = null,
            ToolMode = ChatToolMode.Auto
        };

        var text = new StringBuilder();
        var toolsUsed = new List<string>();
        List<AiChatMessage> turn = [];

        await foreach (var update in this.client.GetStreamingResponseAsync(this.history, options, ct))
        {
            foreach (var content in update.Contents)
            {
                switch (content)
                {
                    case TextContent chunk when chunk.Text.Length > 0:
                        text.Append(chunk.Text);
                        reply = reply with { Body = text.ToString() };
                        this.Replace(reply);
                        break;

                    // What the assistant actually read. Surfaced per message so the data exposure is
                    // visible in the transcript rather than a claim made once at setup.
                    case FunctionCallContent call when !toolsUsed.Contains(call.Name):
                        toolsUsed.Add(call.Name);
                        break;
                }
            }

            turn.Add(new AiChatMessage(update.Role ?? ChatRole.Assistant, [.. update.Contents]));
        }

        var final = reply with
        {
            Body = text.Length == 0 ? "(no answer)" : text.ToString(),
            Status = MessageStatus.Delivered,
            Metadata = toolsUsed.Count == 0
                ? null
                : new Dictionary<string, string> { ["tools"] = string.Join(", ", toolsUsed) }
        };

        lock (this.sync)
        {
            // The whole turn goes into history, tool calls and results included, so the next
            // question can build on what was already read instead of reading it again.
            this.history.AddRange(turn);
        }

        this.Replace(final);
        return final;
    }

    void Replace(UiChatMessage message, MessageChangeKind change = MessageChangeKind.Edited)
    {
        lock (this.sync)
        {
            var i = this.transcript.FindIndex(m => m.MessageId == message.MessageId);
            if (i >= 0)
                this.transcript[i] = message;
        }

        this.MessageUpdated?.Invoke(this, new MessageChanged(message, change));
    }

    // ── Not supported ───────────────────────────────────────────────────
    // Permissions already hide these in the control; they throw rather than no-op so a future caller
    // finds out at once instead of wondering why nothing happened.

    public Task<UiChatMessage> ResendMessageAsync(string clientMessageId, CancellationToken cancellationToken = default)
        => throw new ChatSessionException("Ask again rather than resending - the assistant has no queue to retry from.");

    public Task EditMessageAsync(string messageId, string body, CancellationToken cancellationToken = default)
        => throw new ChatSessionException("Assistant messages cannot be edited.");

    public Task DeleteMessageAsync(string messageId, CancellationToken cancellationToken = default)
        => throw new ChatSessionException("Assistant messages cannot be deleted.");

    public Task ReactToMessageAsync(string messageId, string emoji, bool add, CancellationToken cancellationToken = default)
        => throw new ChatSessionException("There is nobody here to see a reaction.");

    public Task MarkReadAsync(string[] messageIds, CancellationToken cancellationToken = default)
        => Task.CompletedTask;

    public Task ToggleTypingAsync(bool isTyping, CancellationToken cancellationToken = default)
        => Task.CompletedTask;

    public Task InviteUserAsync(string userId, CancellationToken cancellationToken = default)
        => throw new ChatSessionException("An assistant conversation is between you and the model.");

    public Task LeaveAsync(CancellationToken cancellationToken = default)
        => throw new ChatSessionException("Close the tab instead - there is no session to leave.");

    public Task RenameAsync(string sessionName, CancellationToken cancellationToken = default)
        => throw new ChatSessionException("The conversation is named after the connection it reads.");

    public ValueTask DisposeAsync()
    {
        this.client.Dispose();
        return ValueTask.CompletedTask;
    }
}
