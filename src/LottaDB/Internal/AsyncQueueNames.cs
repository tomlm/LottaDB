using System.Security.Cryptography;
using System.Text;
using Azure.Storage.Queues;

namespace Lotta.Internal;

/// <summary>
/// The names of the queues that queued handlers run from. A queue name may hold only lower-case
/// letters, digits and hyphens and be at most 63 characters, while catalog and database names are
/// free text, so the catalog and the database each appear as a short hash:
/// <c>l{catalog:6}{database:7}-{handler}</c>, and <c>…-poison</c> for work that failed too often.
/// </summary>
internal static partial class AsyncQueueNames
{
    public const int MaxHandlerNameLength = 40;
    public const string PoisonSuffix = "-poison";
    private const string Base32 = "abcdefghijklmnopqrstuvwxyz234567";

    /// <summary>3 to 40 lower-case letters and digits, with single hyphens between them, and not ending in <c>-poison</c>.</summary>
    public static bool IsValidHandlerName(string name)
    {
        if (name.Length is < 3 or > MaxHandlerNameLength || name.EndsWith(PoisonSuffix, StringComparison.Ordinal))
            return false;
        for (var n = 0; n < name.Length; n++)
        {
            var c = name[n];
            var hyphenAllowed = n > 0 && n < name.Length - 1 && name[n - 1] != '-';
            if (!(c is >= 'a' and <= 'z' or >= '0' and <= '9' || (c == '-' && hyphenAllowed)))
                return false;
        }
        return true;
    }

    /// <summary>
    /// The queue name for <c>.Queue(handler)</c>, made from the source text of the handler argument
    /// (<c>[CallerArgumentExpression]</c>): its last identifier, without an <c>Async</c> suffix, in
    /// kebab case. <c>Summarize</c> gives <c>summarize</c>, <c>handlers.NotifyFollowersAsync</c>
    /// gives <c>notify-followers</c>, and <c>Handle&lt;Note&gt;</c> gives <c>handle</c>.
    /// </summary>
    /// <exception cref="ArgumentException">
    /// The text is not an identifier or a member access (a lambda, a call, a cast), or the name
    /// made from it is not a valid queue name.
    /// </exception>
    public static string HandlerNameFromExpression(string? expression)
    {
        const string useName = "Name the queue instead: .Queue(\"queue-name\", handler).";
        var text = expression?.Trim() ?? "";
        // Type arguments of a generic method group play no part in the name.
        string previous;
        do
        {
            previous = text;
            text = TypeArguments().Replace(text, "");
        }
        while (text != previous);
        if (!MemberAccess().IsMatch(text))
            throw new ArgumentException(
                $"A queue is named after its handler method, and '{expression}' is not a method name. {useName}", "handler");

        var identifier = text[(text.LastIndexOf('.') + 1)..].Trim().TrimStart('@');
        if (identifier.Length > "Async".Length && identifier.EndsWith("Async", StringComparison.Ordinal))
            identifier = identifier[..^"Async".Length];
        var name = KebabCase(identifier);
        if (!IsValidHandlerName(name))
            throw new ArgumentException(
                $"The queue name '{name}', made from the handler '{expression}', is not a queue name: it needs 3 to 40 " +
                $"lower-case letters and digits, with single hyphens between them. {useName}", "handler");
        return name;
    }

    /// <summary>
    /// <c>NotifyFollowers</c>, <c>notifyFollowers</c> and <c>notify_followers</c> all give
    /// <c>notify-followers</c>; a run of capitals is one word (<c>HTMLParser</c> gives <c>html-parser</c>).
    /// </summary>
    private static string KebabCase(string identifier)
    {
        var name = new StringBuilder();
        for (var n = 0; n < identifier.Length; n++)
        {
            var c = identifier[n];
            if (c == '_')
            {
                if (name.Length > 0 && name[^1] != '-')
                    name.Append('-');
                continue;
            }
            if (char.IsUpper(c) && n > 0 && name.Length > 0 && name[^1] != '-')
            {
                var before = identifier[n - 1];
                var startsWord = char.IsLower(before) || char.IsDigit(before)
                    || (char.IsUpper(before) && n + 1 < identifier.Length && char.IsLower(identifier[n + 1]));
                if (startsWord)
                    name.Append('-');
            }
            name.Append(char.ToLowerInvariant(c));
        }
        return name.ToString().TrimEnd('-');
    }

    [System.Text.RegularExpressions.GeneratedRegex(@"\s*<[^<>]*>")]
    private static partial System.Text.RegularExpressions.Regex TypeArguments();

    /// <summary>An identifier, or identifiers joined by dots: <c>Summarize</c>, <c>this.handlers.Summarize</c>.</summary>
    [System.Text.RegularExpressions.GeneratedRegex(@"^@?[\p{L}_][\p{L}\p{Nd}_]*(\s*\.\s*@?[\p{L}_][\p{L}\p{Nd}_]*)*$")]
    private static partial System.Text.RegularExpressions.Regex MemberAccess();

    /// <summary>What the name of every handler queue in the catalog starts with.</summary>
    public static string CatalogPrefix(string catalog) => "l" + Hash(catalog, 6);

    /// <summary>What the name of every handler queue of one database starts with.</summary>
    public static string DatabasePrefix(string catalog, string databaseId) =>
        CatalogPrefix(catalog) + Hash(catalog + "\n" + databaseId, 7) + "-";

    public static string Queue(string catalog, string databaseId, string handler) => DatabasePrefix(catalog, databaseId) + handler;

    public static string Poison(string catalog, string databaseId, string handler) => Queue(catalog, databaseId, handler) + PoisonSuffix;

    /// <summary>Deletes every queue whose name starts with <paramref name="prefix"/>.</summary>
    public static async Task DeleteQueuesAsync(QueueServiceClient queues, string prefix, CancellationToken cancellationToken)
    {
        var names = new List<string>();
        await foreach (var queue in queues.GetQueuesAsync(prefix: prefix, cancellationToken: cancellationToken).ConfigureAwait(false))
            names.Add(queue.Name);
        foreach (var name in names)
            await queues.GetQueueClient(name).DeleteIfExistsAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>The first <paramref name="length"/> base-32 characters of the SHA-256 of the text.</summary>
    private static string Hash(string text, int length)
    {
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(text));
        var chars = new char[length];
        // Five bits a character, taken from the front of the hash.
        for (var n = 0; n < length; n++)
        {
            var bit = n * 5;
            var value = ((bytes[bit / 8] << 8) | bytes[bit / 8 + 1]) >> (11 - bit % 8) & 31;
            chars[n] = Base32[value];
        }
        return new string(chars);
    }
}
