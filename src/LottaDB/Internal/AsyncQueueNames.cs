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
internal static class AsyncQueueNames
{
    public const int MaxHandlerNameLength = 40;
    private const string PoisonSuffix = "-poison";
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
