using System.Security.Cryptography;
using System.Text;
using Azure.Storage.Queues;

namespace Lotta.Internal;

/// <summary>
/// The names of queued rules, and of the queues their work waits in. There is one queue per
/// object type. A queue name may hold only lower-case letters, digits and hyphens and be at most
/// 63 characters, while catalog, database and type names are free text, so the catalog and the
/// database each appear as a short hash and the type as a readable part plus a hash of its full
/// name: <c>l{catalog:6}{database:7}-{type}-{hash:6}</c>, and <c>…-poison</c> for work that failed
/// too often.
/// </summary>
internal static partial class RuleQueueNames
{
    public const int MaxRuleNameLength = 40;
    public const string PoisonSuffix = "-poison";

    /// <summary>The metadata entry of a queue that holds the full name of its object type.</summary>
    public const string TypeMetadata = "type";

    /// <summary>The queue metadata naming the rule a queue of one rule is for.</summary>
    public const string RuleMetadata = "rule";

    private const int MaxTypeSlugLength = 30;
    private const string Base32 = "abcdefghijklmnopqrstuvwxyz234567";

    /// <summary>3 to 40 lower-case letters and digits, with single hyphens between them.</summary>
    public static bool IsValidRuleName(string name)
    {
        if (name.Length is < 3 or > MaxRuleNameLength)
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
    /// The rule name for <c>.DoQueued(handler)</c> or <c>.ModifyQueued(handler)</c>, made from the source
    /// text of the handler argument (<c>[CallerArgumentExpression]</c>): its last identifier,
    /// without an <c>Async</c> suffix, in kebab case. <c>Summarize</c> gives <c>summarize</c>,
    /// <c>handlers.NotifyFollowersAsync</c> gives <c>notify-followers</c>, and
    /// <c>Handle&lt;Note&gt;</c> gives <c>handle</c>.
    /// </summary>
    /// <param name="expression">The source text of the handler argument.</param>
    /// <param name="verb">The method the handler was given to, for the message: <c>DoQueued</c>.</param>
    /// <exception cref="ArgumentException">
    /// The text is not an identifier or a member access (a lambda, a call, a cast), or the name
    /// made from it is not a valid rule name.
    /// </exception>
    public static string RuleNameFromExpression(string? expression, string verb)
    {
        var useName = $"Name the rule instead: .{verb}(\"rule-name\", handler).";
        var text = WithoutTypeArguments(expression);
        if (!MemberAccess().IsMatch(text))
            throw new ArgumentException(
                $"A rule is named after its handler method, and '{expression}' is not a method name. {useName}", "handler");

        var identifier = text[(text.LastIndexOf('.') + 1)..].Trim().TrimStart('@');
        if (identifier.Length > "Async".Length && identifier.EndsWith("Async", StringComparison.Ordinal))
            identifier = identifier[..^"Async".Length];
        var name = KebabCase(identifier);
        if (!IsValidRuleName(name))
            throw new ArgumentException(
                $"The rule name '{name}', made from the handler '{expression}', is not a rule name: it needs 3 to 40 " +
                $"lower-case letters and digits, with single hyphens between them. {useName}", "handler");
        return name;
    }

    /// <summary>
    /// The rule name for <c>.Materialize(projection)</c>. A method is named as for the other verbs
    /// (<c>ProjectNote</c> gives <c>project-note</c>). A lambda is named after the type it
    /// produces, in kebab case: <c>NoteView</c>, or a collection of them, gives <c>note-view</c>.
    /// </summary>
    /// <param name="expression">The source text of the projection argument.</param>
    /// <param name="produced">The type the projection returns: the object's type, or a collection of it.</param>
    /// <exception cref="ArgumentException">The name made is not a valid rule name.</exception>
    public static string ProjectionName(string? expression, Type produced)
    {
        if (MemberAccess().IsMatch(WithoutTypeArguments(expression)))
            return RuleNameFromExpression(expression, "Materialize");

        var element = produced.GetInterfaces().Append(produced)
            .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>))
            ?.GetGenericArguments()[0] ?? produced;
        var name = KebabCase(element.Name);
        if (!IsValidRuleName(name))
            throw new ArgumentException(
                $"The rule name '{name}', made from the type '{element.Name}' the projection produces, is not a rule name: it needs 3 to 40 " +
                "lower-case letters and digits, with single hyphens between them. Name the rule instead: .Materialize(\"rule-name\", projection).", "projection");
        return name;
    }

    /// <summary>The text of a handler argument without the type arguments of a generic method group, which play no part in the name.</summary>
    private static string WithoutTypeArguments(string? expression)
    {
        var text = expression?.Trim() ?? "";
        string previous;
        do
        {
            previous = text;
            text = TypeArguments().Replace(text, "");
        }
        while (text != previous);
        return text;
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

    /// <summary>What the name of every rule queue in the catalog starts with.</summary>
    public static string CatalogPrefix(string catalog) => "l" + Hash(catalog, 6);

    /// <summary>What the name of every rule queue of one database starts with.</summary>
    public static string DatabasePrefix(string catalog, string databaseId) =>
        CatalogPrefix(catalog) + Hash(catalog + "\n" + databaseId, 7) + "-";

    /// <summary>
    /// The queue for one object type: a readable form of the type's name, and a hash of its full
    /// name so that two types with one name in different namespaces get different queues.
    /// </summary>
    public static string Queue(string catalog, string databaseId, Type type) =>
        DatabasePrefix(catalog, databaseId) + TypeSlug(type.Name, MaxTypeSlugLength) + "-" + Hash(type.FullName ?? type.Name, 6);

    /// <summary>
    /// The queue for one rule of one object type, for a rule with a priority or a concurrency
    /// limit of its own: the type's readable name made shorter, so that with a second hash, for
    /// the rule, the name and its poison queue's still fit in 63 characters.
    /// </summary>
    public static string Queue(string catalog, string databaseId, Type type, string ruleId) =>
        DatabasePrefix(catalog, databaseId) + TypeSlug(type.Name, MaxTypeSlugLength - 7) + "-" + Hash(type.FullName ?? type.Name, 6) + Hash(ruleId, 6);

    /// <summary>Where the work of a queue goes when it has failed too often.</summary>
    public static string Poison(string queue) => queue + PoisonSuffix;

    /// <summary>A type's name as a queue name allows it: <c>BlobPhoto</c> gives <c>blob-photo</c>.</summary>
    private static string TypeSlug(string typeName, int maxLength)
    {
        var slug = new StringBuilder();
        foreach (var c in KebabCase(typeName))
        {
            if (c is >= 'a' and <= 'z' or >= '0' and <= '9')
                slug.Append(c);
            else if (slug.Length > 0 && slug[^1] != '-')
                slug.Append('-');   // generic arity marks, nested-type marks and the like
            if (slug.Length == maxLength)
                break;
        }
        var text = slug.ToString().Trim('-');
        return text.Length == 0 ? "t" : text;
    }

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
