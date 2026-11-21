using Azure.Data.Tables;
using Iciclecreek.Lucene.Net.Vector;
using Microsoft.Extensions.AI;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;

namespace Lotta.Internal;

/// <summary>
/// A vector kept on an object's row: the bytes Lucene indexes (as <see cref="VectorSerializer"/>
/// writes them), the hash of the model and text they were computed from, and the model.
/// </summary>
internal sealed record StoredVector(byte[] Bytes, string Hash, string? Model);

/// <summary>
/// One field of a type that gets a vector: its Lucene field name and how to read its text from an object.
/// </summary>
internal sealed record VectorField(string Name, Func<object, string?> Text);

/// <summary>
/// The vectors of an object, kept on its row beside the object: <c>_{Field}_vector</c> and
/// <c>_{Field}_hash</c> per vector field, and <c>_model</c> for the row. A vector is valid for
/// as long as its hash is the hash of the current model and the field's current text, so a
/// save of unchanged text, a rebuild of the index, or a copy of the object reuses it, and the
/// model is called only for text it has not embedded.
/// </summary>
internal static class StoredVectors
{
    private const string VectorSuffix = "_vector";
    private const string HashSuffix = "_hash";

    /// <summary>The row's embedding model id, for diagnostics.</summary>
    public const string Model = "_model";

    /// <summary>The row lists no vectors.</summary>
    public static readonly IReadOnlyDictionary<string, StoredVector> None = new Dictionary<string, StoredVector>();

    public static string VectorColumn(string field) => "_" + field + VectorSuffix;

    public static string HashColumn(string field) => "_" + field + HashSuffix;

    /// <summary>The hash a vector of this text from this model is stored under.</summary>
    public static string Hash(string model, string text)
    {
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(model + "\n" + text));
        return Convert.ToBase64String(bytes);
    }

    /// <summary>The id of the model a generator embeds with: its metadata's model id, else its type.</summary>
    public static string ModelId(IEmbeddingGenerator<string, Embedding<float>> generator)
        => generator.GetService<EmbeddingGeneratorMetadata>()?.DefaultModelId
           ?? generator.GetType().FullName
           ?? generator.GetType().Name;

    /// <summary>
    /// The fields of a type that get a vector, in the order the mapper indexes them: its vector
    /// properties, and the <c>_content</c> field when the type has no default search property.
    /// None when there is no embedding generator.
    /// </summary>
    public static IReadOnlyList<VectorField> FieldsOf(TypeMetadata? meta, bool hasGenerator)
    {
        if (meta == null || !hasGenerator)
            return [];
        var fields = new List<VectorField>();
        foreach (var indexed in meta.IndexedProperties)
        {
            // The key is indexed as itself, and a string collection by its own mapper: neither gets a vector.
            if (!indexed.IsVectorField || indexed.Property == meta.KeyProperty || TypeUtils.IsStringCollection(indexed.Property.PropertyType))
                continue;
            var property = indexed.Property;
            fields.Add(new VectorField(property.Name, obj => property.GetValue(obj) as string));
        }
        if (meta.DefaultSearchProperty == null)
        {
            var contentProperties = ContentProperties(meta);
            fields.Add(new VectorField(StorageFields.Content, obj => ContentText.Build(obj, contentProperties)));
        }
        return fields;
    }

    /// <summary>The properties whose text makes up the <c>_content</c> field.</summary>
    public static PropertyInfo[] ContentProperties(TypeMetadata meta) => meta.IndexedProperties
        .Where(p => !p.IsNotAnalyzed && (p.Property.PropertyType == typeof(string) || TypeUtils.IsStringCollection(p.Property.PropertyType)))
        .Select(p => p.Property)
        .ToArray();

    /// <summary>The vectors a row holds, by field.</summary>
    public static IReadOnlyDictionary<string, StoredVector> Read(TableEntity entity)
    {
        Dictionary<string, StoredVector>? vectors = null;
        var model = entity.TryGetValue(Model, out var m) ? m as string : null;
        foreach (var (column, value) in entity)
        {
            if (value is not byte[] bytes || !column.StartsWith('_') || !column.EndsWith(VectorSuffix, StringComparison.Ordinal))
                continue;
            var field = column[1..^VectorSuffix.Length];
            if (entity.TryGetValue(HashColumn(field), out var hash) && hash is string h)
                (vectors ??= new(StringComparer.Ordinal))[field] = new StoredVector(bytes, h, model);
        }
        return vectors ?? None;
    }

    /// <summary>Writes an object's vectors as columns of its row.</summary>
    public static void Write(TableEntity entity, IReadOnlyDictionary<string, StoredVector> vectors)
    {
        foreach (var (field, vector) in vectors)
        {
            entity[VectorColumn(field)] = vector.Bytes;
            entity[HashColumn(field)] = vector.Hash;
            if (vector.Model != null)
                entity[Model] = vector.Model;
        }
    }
}
