namespace Shiny.DocumentDb;

/// <summary>
/// Generates the time-ordered ids the processor reads FIFO by.
/// </summary>
/// <remarks>
/// <para>
/// These are RFC 9562 version-7 UUIDs, but <b>not</b> <see cref="Guid.CreateVersion7()"/>: that fills the whole
/// sub-millisecond portion with randomness, so several messages enqueued in the same millisecond — which is the
/// normal case for one unit of work — sort in an arbitrary order. FIFO delivery and
/// <c>OutboxOptions.OrderedPartitions</c> both rest on ordering by id, so the 12-bit <c>rand_a</c> field is
/// used as a per-millisecond counter instead (the "monotonic random" method the spec describes). The result is
/// still a valid v7 UUID and still <see cref="Guid.Parse(string)"/>-able.
/// </para>
/// <para>
/// Monotonic <i>within a process</i>. Two processes enqueueing in the same millisecond can still interleave —
/// ordering is at millisecond granularity across writers, which is the ordering guarantee the outbox documents.
/// </para>
/// </remarks>
static class OutboxId
{
    // 12 bits of rand_a: 4096 ids per millisecond before the generator borrows from the next one.
    const int MaxSequence = 0xFFF;

    static readonly Lock Gate = new();
    static long lastMilliseconds;
    static int sequence;

    /// <summary>The next id, in "N" (32 hex chars, no dashes) form — the same storage shape every other
    /// Guid id in the library uses, and lexicographically ordered because the timestamp leads.</summary>
    public static string Next()
    {
        long milliseconds;
        int seq;

        lock (Gate)
        {
            var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            if (now > lastMilliseconds)
            {
                lastMilliseconds = now;
                sequence = 0;
            }
            else
            {
                // Same millisecond, or a clock that went backwards: keep counting forward from where we were,
                // borrowing the next millisecond if the counter is exhausted. Ordering never regresses.
                sequence++;
                if (sequence > MaxSequence)
                {
                    lastMilliseconds++;
                    sequence = 0;
                }
            }
            milliseconds = lastMilliseconds;
            seq = sequence;
        }

        Span<byte> bytes = stackalloc byte[16];
        bytes[0] = (byte)(milliseconds >> 40);
        bytes[1] = (byte)(milliseconds >> 32);
        bytes[2] = (byte)(milliseconds >> 24);
        bytes[3] = (byte)(milliseconds >> 16);
        bytes[4] = (byte)(milliseconds >> 8);
        bytes[5] = (byte)milliseconds;
        bytes[6] = (byte)(0x70 | ((seq >> 8) & 0x0F));   // version 7 + counter high nibble
        bytes[7] = (byte)seq;                            // counter low byte

        Random.Shared.NextBytes(bytes[8..]);
        bytes[8] = (byte)((bytes[8] & 0x3F) | 0x80);     // RFC 9562 variant

        return new Guid(bytes, bigEndian: true).ToString("N");
    }
}
