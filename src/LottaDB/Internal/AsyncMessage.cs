using System.Text.Json;

namespace Lotta.Internal;

/// <summary>
/// What a queued handler's message holds: only what is needed to run the handler, never the
/// object itself. The object is fetched when the handler runs, so its size never matters and the
/// handler always sees it as it is then.
/// </summary>
internal sealed class AsyncMessage
{
    /// <summary>The full name of the object's type when it was saved.</summary>
    public string Type { get; set; } = "";

    /// <summary>The object's key.</summary>
    public string Key { get; set; } = "";

    /// <summary>The database instance that sent the message; new each time a database is opened.</summary>
    public string Sender { get; set; } = "";

    /// <summary>The sender's running count, so that it can tell an older message of its own from a newer one.</summary>
    public long Seq { get; set; }

    /// <summary>How many queued handlers led to this message: 0 for a save from outside any handler.</summary>
    public int Hop { get; set; }

    /// <summary>How many runs for this message have failed.</summary>
    public int Attempts { get; set; }

    /// <summary>
    /// How many runs for this message found another process holding the write lock. Not
    /// failures: they only make the wait before the next try longer.
    /// </summary>
    public int Waits { get; set; }

    /// <summary>What the last failure said.</summary>
    public string? LastError { get; set; }

    /// <summary>The message as text. The default encoder escapes everything outside ASCII, so any queue encoding carries it.</summary>
    public string ToText() => JsonSerializer.Serialize(this);

    /// <summary>The message in the text, or null when the text is not one.</summary>
    public static AsyncMessage? TryParse(string text)
    {
        try
        {
            var message = JsonSerializer.Deserialize<AsyncMessage>(text);
            return message is { Key.Length: > 0 } ? message : null;
        }
        catch (JsonException)
        {
            return null;
        }
    }
}
