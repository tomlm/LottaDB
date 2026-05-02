using Azure.Data.Tables;

namespace Lotta.Internal;

/// <summary>
/// Extension methods for reading/writing serialized object bytes on <see cref="TableEntity"/>,
/// with automatic splitting across multiple properties for objects exceeding 64KB.
/// </summary>
internal static class TableEntityExtensions
{
    internal const string TypeProperty = "Type";
    internal const string ObjectPropertyPrefix = "Object";
    internal const int MaxPropertySize = 63 * 1024; // 63KB to stay safely under 64KB limit

    /// <summary>
    /// Gets the reassembled bytes from one or more Object/Object2/Object3... properties.
    /// </summary>
    internal static byte[] GetObjectBytes(this TableEntity entity)
    {
        var first = entity.TryGetValue(ObjectPropertyPrefix, out var rawVal) ? (byte[])rawVal : null;
        if (first == null) return Array.Empty<byte>();

        // Check for split properties
        var chunks = new List<byte[]> { first };
        for (int i = 2; ; i++)
        {
            var key = $"{ObjectPropertyPrefix}{i}";
            if (entity.TryGetValue(key, out var chunk))
            {
                var chunkBytes = (byte[])chunk;
                if (chunkBytes != null)
                    chunks.Add(chunkBytes);
                else
                    break;
            }
            else
                break;
        }

        if (chunks.Count == 1) return first;

        // Reassemble
        var totalLength = chunks.Sum(c => c.Length);
        var result = new byte[totalLength];
        var offset = 0;
        foreach (var chunkItem in chunks)
        {
            Buffer.BlockCopy(chunkItem, 0, result, offset, chunkItem.Length);
            offset += chunkItem.Length;
        }
        return result;
    }

    /// <summary>
    /// Sets the serialized bytes, splitting across multiple properties if needed.
    /// </summary>
    internal static void SetObjectBytes(this TableEntity entity, byte[] data)
    {
        if (data.Length <= MaxPropertySize)
        {
            entity[ObjectPropertyPrefix] = data;
            return;
        }

        // Split into chunks
        int chunkIndex = 0;
        for (int offset = 0; offset < data.Length; offset += MaxPropertySize)
        {
            var length = Math.Min(MaxPropertySize, data.Length - offset);
            var chunk = new byte[length];
            Buffer.BlockCopy(data, offset, chunk, 0, length);

            var key = chunkIndex == 0 ? ObjectPropertyPrefix : $"{ObjectPropertyPrefix}{chunkIndex + 1}";
            entity[key] = chunk;
            chunkIndex++;
        }
    }
}
