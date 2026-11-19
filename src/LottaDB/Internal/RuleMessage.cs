using System.Text.Json;
using System.Text.Json.Serialization;

namespace Lotta.Internal;

/// <summary>
/// What a message in a type's queue holds: which object was saved, and which queued rules that
/// save matched. Never the object itself: it is read when the rules run, so its size never matters
/// and the rules always see it as it is then. All retry state is here too, so a rule that fails is
/// retried by replacing the message in place with one that lists only what is left to do.
/// <code>{ "key": "X1", "etag": "e7", "rules": ["Note/b"], "attempts": { "Note/b": 1 }, "lastError": { "Note/b": "timeout calling model" } }</code>
/// </summary>
internal sealed class RuleMessage
{
    /// <summary>The object's key.</summary>
    [JsonPropertyName("key")]
    public string Key { get; set; } = "";

    /// <summary>
    /// The object's ETag after the save that sent the message. It names that save (it is part of
    /// the idempotency key); the rules still run when the object has since moved on.
    /// </summary>
    [JsonPropertyName("etag")]
    public string? ETag { get; set; }

    /// <summary>The rules still to run, by identity (<c>Note/summarize</c>).</summary>
    [JsonPropertyName("rules")]
    public List<string> Rules { get; set; } = [];

    /// <summary>How many runs of each rule have failed; a rule that has not failed is not listed.</summary>
    [JsonPropertyName("attempts")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public Dictionary<string, int>? Attempts { get; set; }

    /// <summary>What each rule's last failure said.</summary>
    [JsonPropertyName("lastError")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public Dictionary<string, string>? LastError { get; set; }

    /// <summary>How many queued rules led to this message: 0 for a save from outside any rule.</summary>
    [JsonPropertyName("hop")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    public int Hop { get; set; }

    /// <summary>
    /// How many runs for this message found another process holding the write lock. Not
    /// failures: they only make the wait before the next try longer.
    /// </summary>
    [JsonPropertyName("waits")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    public int Waits { get; set; }

    /// <summary>
    /// For a delayed rule (<c>After</c>, <c>At</c>): when it is to run. A message that comes up
    /// before then is hidden again, which is how a wait longer than a queue can hide a message
    /// for is made.
    /// </summary>
    [JsonPropertyName("due")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public DateTimeOffset? Due { get; set; }

    /// <summary>
    /// For the rules of a delete (<c>WhenDeleted(...).DoQueued</c>): the key of the tombstone, the
    /// row that keeps the object as it was until its rules have run. The message owns it, and
    /// removes it when no rule is left. Null for the rules of a save.
    /// </summary>
    [JsonPropertyName("tombstone")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? Tombstone { get; set; }

    /// <summary>How many runs of the rule have failed.</summary>
    public int AttemptsOf(string rule) => Attempts != null && Attempts.TryGetValue(rule, out var count) ? count : 0;

    /// <summary>The message as text. The default encoder escapes everything outside ASCII, so any queue encoding carries it.</summary>
    public string ToText() => JsonSerializer.Serialize(this);

    /// <summary>The message in the text, or null when the text is not one.</summary>
    public static RuleMessage? TryParse(string text)
    {
        try
        {
            var message = JsonSerializer.Deserialize<RuleMessage>(text);
            return message is { Key.Length: > 0, Rules: not null } ? message : null;
        }
        catch (JsonException)
        {
            return null;
        }
    }
}

/// <summary>
/// A piece of work set aside in a type's poison queue: one rule, for one object, that failed too
/// often (or that went through too many rules to get here).
/// </summary>
internal sealed class PoisonRecord
{
    /// <summary>The full name of the object's type.</summary>
    [JsonPropertyName("type")]
    public string Type { get; set; } = "";

    /// <summary>The object's key.</summary>
    [JsonPropertyName("key")]
    public string Key { get; set; } = "";

    /// <summary>The object's ETag after the save that queued the work.</summary>
    [JsonPropertyName("etag")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? ETag { get; set; }

    /// <summary>The rule, by identity (<c>Note/summarize</c>).</summary>
    [JsonPropertyName("rule")]
    public string Rule { get; set; } = "";

    /// <summary>What the last failure said.</summary>
    [JsonPropertyName("error")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? Error { get; set; }

    /// <summary>How many runs failed.</summary>
    [JsonPropertyName("attempts")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    public int Attempts { get; set; }

    /// <summary>
    /// For a rule of a delete: the key of a tombstone of its own, which keeps the object as it was
    /// so the work can be retried. Removed when the work is discarded, or run after a retry.
    /// </summary>
    [JsonPropertyName("tombstone")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? Tombstone { get; set; }

    public string ToText() => JsonSerializer.Serialize(this);

    /// <summary>The record in the text, or null when the text is not one (a message that could not be read, kept as it was).</summary>
    public static PoisonRecord? TryParse(string text)
    {
        try
        {
            var record = JsonSerializer.Deserialize<PoisonRecord>(text);
            return record is { Key.Length: > 0, Rule.Length: > 0 } ? record : null;
        }
        catch (JsonException)
        {
            return null;
        }
    }
}
