using System.Runtime.CompilerServices;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.Logging.Abstractions;
using Shiny.Blazor.Controls.Chat;
using ShinyDocDbMyAdmin.Services;
using AiChatMessage = Microsoft.Extensions.AI.ChatMessage;
using UiChatMessage = Shiny.Blazor.Controls.Chat.ChatMessage;

namespace ShinyDocDbMyAdmin.Tests;

/// <summary>
/// The bridge between ChatView's session contract and an <see cref="IChatClient"/>, driven by a fake
/// client so nothing here touches a network or a key.
/// </summary>
public sealed class AiChatSessionTests
{
    /// <summary>
    /// Streams a canned reply in chunks, optionally preceded by a tool call, and records what it was
    /// asked so the tests can assert on the conversation that was actually sent.
    /// </summary>
    sealed class FakeChatClient(string reply, string? toolCalled = null) : IChatClient
    {
        public List<AiChatMessage> LastRequest { get; } = [];
        public ChatOptions? LastOptions { get; private set; }
        public int Calls { get; private set; }

        public Task<ChatResponse> GetResponseAsync(
            IEnumerable<AiChatMessage> messages, ChatOptions? options = null, CancellationToken cancellationToken = default)
        {
            this.Record(messages, options);
            return Task.FromResult(new ChatResponse(new AiChatMessage(ChatRole.Assistant, reply)));
        }

        public async IAsyncEnumerable<ChatResponseUpdate> GetStreamingResponseAsync(
            IEnumerable<AiChatMessage> messages, ChatOptions? options = null,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            this.Record(messages, options);

            if (toolCalled is not null)
            {
                yield return new ChatResponseUpdate(ChatRole.Assistant,
                    new AIContent[] { new FunctionCallContent("call-1", toolCalled, new Dictionary<string, object?>()) });
            }

            // Chunked, so the streaming path is genuinely exercised rather than a single shot.
            foreach (var word in reply.Split(' '))
                yield return new ChatResponseUpdate(ChatRole.Assistant, word + " ");

            await Task.CompletedTask;
        }

        void Record(IEnumerable<AiChatMessage> messages, ChatOptions? options)
        {
            this.Calls++;
            this.LastRequest.Clear();
            this.LastRequest.AddRange(messages);
            this.LastOptions = options;
        }

        public object? GetService(Type serviceType, object? serviceKey = null) => null;
        public void Dispose() { }
    }

    static AiChatSession Session(IChatClient client, IReadOnlyList<AITool>? tools = null)
        => new(
            "p1",
            "Demo",
            client,
            tools ?? [],
            "You are a test.",
            NullLogger.Instance);

    static OutgoingMessage Ask(string text) => new(text, null, Guid.NewGuid().ToString("N"));

    static async Task<IReadOnlyList<UiChatMessage>> Transcript(AiChatSession session)
    {
        var page = await session.GetMessagesAsync(null, MessagePageDirection.Older, 100, TestContext.Current.CancellationToken);
        return page.Messages;
    }

    // ── Sending ─────────────────────────────────────────────────────────

    [Fact]
    public async Task Sending_PutsBothTurnsInTheTranscript()
    {
        var client = new FakeChatClient("Three zones.");
        await using var session = Session(client);

        await session.SendMessageAsync(Ask("How many zones?"), TestContext.Current.CancellationToken);

        var messages = await Transcript(session);
        Assert.Equal(2, messages.Count);
        Assert.Equal(AiChatSession.UserId, messages[0].SenderId);
        Assert.Equal("How many zones?", messages[0].Body);
        Assert.Equal(AiChatSession.AssistantId, messages[1].SenderId);
        Assert.Equal("Three zones.", messages[1].Body?.Trim());
    }

    [Fact]
    public async Task TheReply_IsDeliveredNotLeftSending()
    {
        var client = new FakeChatClient("Done.");
        await using var session = Session(client);

        await session.SendMessageAsync(Ask("Hi"), TestContext.Current.CancellationToken);

        var messages = await Transcript(session);
        Assert.Equal(MessageStatus.Delivered, messages[1].Status);
    }

    [Fact]
    public async Task TheReply_StreamsThroughMessageUpdated()
    {
        var client = new FakeChatClient("one two three four");
        await using var session = Session(client);

        var updates = 0;
        session.MessageUpdated += (_, _) => updates++;

        await session.SendMessageAsync(Ask("count"), TestContext.Current.CancellationToken);

        // One per chunk plus the final status flip - a single update would mean the answer appeared
        // all at once after the wait, which is the behaviour the placeholder exists to avoid.
        Assert.True(updates > 1, $"expected several streaming updates, saw {updates}");
    }

    [Fact]
    public async Task APlaceholderArrivesBeforeTheAnswer()
    {
        var client = new FakeChatClient("Eventually.");
        await using var session = Session(client);

        UiChatMessage? firstReceived = null;
        session.MessageReceived += (_, m) => firstReceived ??= m;

        await session.SendMessageAsync(Ask("Hi"), TestContext.Current.CancellationToken);

        Assert.NotNull(firstReceived);
        Assert.Equal(AiChatSession.AssistantId, firstReceived.SenderId);
        Assert.Equal(MessageStatus.Sending, firstReceived.Status);
    }

    [Fact]
    public async Task ToolCalls_AreRecordedOnTheReply()
    {
        var client = new FakeChatClient("Read it.", toolCalled: "browse_documents");
        await using var session = Session(client);

        await session.SendMessageAsync(Ask("what is in there"), TestContext.Current.CancellationToken);

        // This is what the per-message footer renders: the exposure made visible per turn.
        var messages = await Transcript(session);
        Assert.NotNull(messages[1].Metadata);
        Assert.Contains("browse_documents", messages[1].Metadata!["tools"]);
    }

    [Fact]
    public async Task NoToolCalls_MeansNoToolMetadata()
    {
        var client = new FakeChatClient("Just chatting.");
        await using var session = Session(client);

        await session.SendMessageAsync(Ask("hello"), TestContext.Current.CancellationToken);

        var messages = await Transcript(session);
        Assert.Null(messages[1].Metadata);
    }

    [Fact]
    public async Task TheSystemPrompt_LeadsTheConversation()
    {
        var client = new FakeChatClient("ok");
        await using var session = Session(client);

        await session.SendMessageAsync(Ask("hi"), TestContext.Current.CancellationToken);

        Assert.Equal(ChatRole.System, client.LastRequest[0].Role);
        Assert.Contains("test", client.LastRequest[0].Text, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task EarlierTurns_AreCarriedIntoTheNextRequest()
    {
        var client = new FakeChatClient("ok");
        await using var session = Session(client);

        await session.SendMessageAsync(Ask("first"), TestContext.Current.CancellationToken);
        await session.SendMessageAsync(Ask("second"), TestContext.Current.CancellationToken);

        // Without history the assistant would re-read everything for every follow-up question.
        Assert.Contains(client.LastRequest, m => m.Text?.Contains("first") == true);
        Assert.Contains(client.LastRequest, m => m.Text?.Contains("second") == true);
    }

    [Fact]
    public async Task NothingIsSent_UntilAMessageIsSent()
    {
        var client = new FakeChatClient("ok");
        await using var session = Session(client);

        // The promise the UI makes: configuring and opening the assistant transmits nothing.
        _ = await Transcript(session);
        Assert.Equal(0, client.Calls);

        await session.SendMessageAsync(Ask("now"), TestContext.Current.CancellationToken);
        Assert.Equal(1, client.Calls);
    }

    [Fact]
    public async Task AnEmptyMessage_IsRefused()
    {
        var client = new FakeChatClient("ok");
        await using var session = Session(client);

        await Assert.ThrowsAsync<ChatSessionException>(
            () => session.SendMessageAsync(Ask("   "), TestContext.Current.CancellationToken));

        Assert.Equal(0, client.Calls);
    }

    [Fact]
    public async Task AProviderFailure_MarksTheReplyFailedRatherThanThrowing()
    {
        await using var session = Session(new ThrowingClient());

        await session.SendMessageAsync(Ask("hi"), TestContext.Current.CancellationToken);

        var messages = await Transcript(session);
        Assert.Equal(MessageStatus.Failed, messages[1].Status);
        Assert.Contains("provider is down", messages[1].StatusReason!);
    }

    sealed class ThrowingClient : IChatClient
    {
        public Task<ChatResponse> GetResponseAsync(
            IEnumerable<AiChatMessage> messages, ChatOptions? options = null, CancellationToken cancellationToken = default)
            => throw new InvalidOperationException("the provider is down");

        public IAsyncEnumerable<ChatResponseUpdate> GetStreamingResponseAsync(
            IEnumerable<AiChatMessage> messages, ChatOptions? options = null, CancellationToken cancellationToken = default)
            => throw new InvalidOperationException("the provider is down");

        public object? GetService(Type serviceType, object? serviceKey = null) => null;
        public void Dispose() { }
    }

    // ── Paging ──────────────────────────────────────────────────────────

    [Fact]
    public async Task Paging_WalksBackwardsThroughTheTranscript()
    {
        var client = new FakeChatClient("ok");
        await using var session = Session(client);

        for (var i = 0; i < 5; i++)
            await session.SendMessageAsync(Ask($"q{i}"), TestContext.Current.CancellationToken);

        var newest = await session.GetMessagesAsync(null, MessagePageDirection.Older, 4, TestContext.Current.CancellationToken);
        Assert.Equal(4, newest.Messages.Count);
        Assert.True(newest.HasMore);

        var older = await session.GetMessagesAsync(
            newest.Messages[0].MessageId, MessagePageDirection.Older, 4, TestContext.Current.CancellationToken);
        Assert.NotEmpty(older.Messages);
    }

    // ── Permissions ─────────────────────────────────────────────────────

    [Fact]
    public async Task OnlySendingIsPermitted()
    {
        var client = new FakeChatClient("ok");
        await using var session = Session(client);

        Assert.Equal(ChatSessionPermissions.CanSendMessages, session.Info.Permissions);

        // No composition toolbar: this is a question box, not a message composer.
        Assert.Equal(MessageBodyPermissions.None, session.Info.BodyPermissions);
    }

    [Theory]
    [InlineData("edit")]
    [InlineData("delete")]
    [InlineData("react")]
    [InlineData("invite")]
    [InlineData("rename")]
    [InlineData("leave")]
    public async Task UnsupportedOperations_Throw(string operation)
    {
        var client = new FakeChatClient("ok");
        await using var session = Session(client);
        var ct = TestContext.Current.CancellationToken;

        Func<Task> call = operation switch
        {
            "edit" => () => session.EditMessageAsync("m", "b", ct),
            "delete" => () => session.DeleteMessageAsync("m", ct),
            "react" => () => session.ReactToMessageAsync("m", "👍", true, ct),
            "invite" => () => session.InviteUserAsync("u", ct),
            "rename" => () => session.RenameAsync("n", ct),
            _ => () => session.LeaveAsync(ct)
        };

        await Assert.ThrowsAsync<ChatSessionException>(call);
    }

    [Fact]
    public void SessionIds_CarryTheScope()
    {
        Assert.Equal("p1", AiChatSessionProvider.SessionIdFor("p1"));
        Assert.Equal("p1|documents|Zone", AiChatSessionProvider.SessionIdFor("p1", "documents", "Zone"));

        // A half-specified scope is a connection-wide conversation, not a broken id.
        Assert.Equal("p1", AiChatSessionProvider.SessionIdFor("p1", "documents", null));
    }
}
