using System.Collections.Concurrent;
using System.Text.Json;
using Iciclecreek.Lucene.Net.Vector;
using Lotta.Internal;

namespace Lotta;

// Vectors kept on the object's row (_{Field}_vector, _{Field}_hash, _model), beside the object
// and not in it, the way _produced is. Each is valid while its hash is the hash of the current
// model and the field's current text. A save writes them back, computing only the ones whose text
// is new, and indexing copies them into Lucene: the model is never called by the index, so a
// rebuild, a schema change or a re-save of unchanged text embeds nothing.
public partial class LottaDB
{
    private readonly ConcurrentDictionary<Type, IReadOnlyList<VectorField>> _vectorFields = new();
    private string? _embeddingModelId;

    /// <summary>The id of the catalog's embedding model, which every stored vector's hash includes; null without one.</summary>
    internal string? EmbeddingModelId => _lottaCatalog.EmbeddingGenerator is { } generator
        ? _embeddingModelId ??= StoredVectors.ModelId(generator)
        : null;

    /// <summary>The fields of a type that get a vector; none without an embedding generator.</summary>
    internal IReadOnlyList<VectorField> VectorFieldsOf(Type type) => _vectorFields.GetOrAdd(type, t =>
        StoredVectors.FieldsOf(_metadata.TryGetValue(t, out var meta) ? meta : null, _lottaCatalog.EmbeddingGenerator != null));

    /// <summary>
    /// The vector stored with an object for one of its vector fields, read from its row with it:
    /// search near it with <c>Similar(vector)</c> to find what is like the object, with nothing
    /// embedded again. Null when the object has no stored vector for the field, or when the one it
    /// has is not that of the field's text as it is now or of the catalog's embedding model (it was
    /// edited since, or is still waiting for the <c>embed</c> rule).
    /// </summary>
    /// <param name="item">An object read from its row (by Get or GetMany); a search result is made from the index and carries none.</param>
    /// <param name="field">The vector property's name (<c>nameof(Note.Title)</c>); null for the
    /// object's <c>_content</c> field, which a type without a default search property has.</param>
    public float[]? GetVector(object item, string? field = null)
    {
        ArgumentNullException.ThrowIfNull(item);
        if (EmbeddingModelId is not { } model || item.GetVectors() is not { } vectors)
            return null;
        var name = field ?? Internal.StorageFields.Content;
        if (!vectors.TryGetValue(name, out var stored)
            || VectorFieldsOf(item.GetType()).FirstOrDefault(f => f.Name == name)?.Text(item) is not { Length: > 0 } text
            || stored.Hash != StoredVectors.Hash(model, text))
            return null;
        return VectorSerializer.FromBytes(stored.Bytes);
    }

    /// <summary>The name of the rule LottaDB registers to embed a type's vector fields.</summary>
    internal const string EmbedRuleName = "embed";

    /// <summary>
    /// Whether vectors of a type are computed by a queued rule. When they are not (the storage
    /// has no queues, so no rule can be registered), a save embeds new text itself.
    /// </summary>
    private bool EmbedsFromQueue(Type type) =>
        HasQueuedRules && QueuedRulesFor(type).Any(r => r.Name == EmbedRuleName);

    /// <summary>
    /// Before an object's row is written: puts on it the vectors the row is to carry, and notes
    /// whether any field's text has none yet. A stored vector whose text is unchanged is kept; one
    /// whose text changed is dropped, and the <c>embed</c> rule computes it. The stored vectors are
    /// read from the row when the object in hand does not know them (it was not read from its row).
    /// </summary>
    /// <param name="known">What the stored row holds, when the caller has just read it; null to use what the object knows, or find out.</param>
    private async Task PrepareVectorsAsync(object entity, string key, IReadOnlyDictionary<string, StoredVector>? known, CancellationToken cancellationToken)
    {
        if (entity is JsonDocument || _lottaCatalog.EmbeddingGenerator == null || VectorFieldsOf(entity.GetType()).Count == 0)
            return;
        await UpdateVectorsAsync(entity, key, known, embed: !EmbedsFromQueue(entity.GetType()), cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// What <see cref="Rule.EmbedVectors"/> does: embeds each vector field whose text has no vector,
    /// all of them in one call, and puts the vectors on the object. Nothing is written here; the
    /// queued rule writes the object back.
    /// </summary>
    internal async Task EmbedVectorsAsync(object item, CancellationToken cancellationToken)
    {
        if (item is JsonDocument || _lottaCatalog.EmbeddingGenerator == null || VectorFieldsOf(item.GetType()).Count == 0)
            return;
        var key = item.GetKey() ?? GetMeta(item.GetType()).GetKey(item);
        await UpdateVectorsAsync(item, key, known: null, embed: true, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// What <see cref="Rule.EmbedVectors"/> does, for several objects with one call to the
    /// embedding generator: the batched run of the <c>embed</c> rule. Nothing is written here.
    /// </summary>
    internal async Task EmbedVectorsAsync(IReadOnlyList<(object Item, string Key)> items, CancellationToken cancellationToken)
    {
        if (_lottaCatalog.EmbeddingGenerator == null)
            return;
        var plans = new List<VectorPlan>();
        foreach (var (item, key) in items)
        {
            if (item is not JsonDocument && VectorFieldsOf(item.GetType()).Count > 0)
                plans.Add(await PlanVectorsAsync(item, key, known: null, cancellationToken).ConfigureAwait(false));
        }
        await EmbedAsync(plans, cancellationToken).ConfigureAwait(false);
        foreach (var plan in plans)
            plan.Apply();
    }

    /// <summary>The vectors an object is to carry, and the texts of its fields that have none yet.</summary>
    private sealed class VectorPlan(object entity, Dictionary<string, StoredVector> vectors, List<(string Field, string Text, string Hash)>? stale)
    {
        public object Entity { get; } = entity;
        public Dictionary<string, StoredVector> Vectors { get; } = vectors;
        public List<(string Field, string Text, string Hash)>? Stale { get; set; } = stale;

        /// <summary>Puts the vectors on the object, noting whether a field is left without one.</summary>
        public void Apply()
        {
            Entity.SetVectors(Vectors);
            Entity.SetVectorsStale(Stale != null);
        }
    }

    /// <summary>
    /// Embeds the texts the plans have no vector for, all of them in one call to the embedding
    /// generator (a text that comes more than once is embedded once), and adds the vectors to the plans.
    /// </summary>
    private async Task EmbedAsync(IReadOnlyList<VectorPlan> plans, CancellationToken cancellationToken)
    {
        var texts = new List<string>();
        var index = new Dictionary<string, int>(StringComparer.Ordinal);   // by hash: which embedding is the text's
        foreach (var plan in plans)
        {
            foreach (var (_, text, hash) in plan.Stale ?? [])
            {
                if (index.TryAdd(hash, texts.Count))
                    texts.Add(text);
            }
        }
        if (texts.Count == 0)
            return;

        var embeddings = await _lottaCatalog.EmbeddingGenerator!.GenerateAsync(texts, cancellationToken: cancellationToken).ConfigureAwait(false);
        if (embeddings.Count != texts.Count)
            throw new InvalidOperationException($"The embedding generator returned {embeddings.Count} embeddings for {texts.Count} texts.");
        var model = EmbeddingModelId!;
        foreach (var plan in plans)
        {
            foreach (var (field, _, hash) in plan.Stale ?? [])
                plan.Vectors[field] = new StoredVector(VectorSerializer.ToBytes(embeddings[index[hash]].Vector.ToArray()), hash, model);
            plan.Stale = null;
        }
    }

    /// <summary>
    /// Keeps the object's stored vectors that are still those of its text, embeds the rest when
    /// <paramref name="embed"/> is set, and notes whether any field is left without a vector.
    /// </summary>
    private async Task UpdateVectorsAsync(object entity, string key, IReadOnlyDictionary<string, StoredVector>? known, bool embed, CancellationToken cancellationToken)
    {
        var plan = await PlanVectorsAsync(entity, key, known, cancellationToken).ConfigureAwait(false);
        if (embed)
            await EmbedAsync([plan], cancellationToken).ConfigureAwait(false);
        plan.Apply();
    }

    /// <summary>
    /// Which of the object's stored vectors are still those of its text, and which texts have none.
    /// </summary>
    private async Task<VectorPlan> PlanVectorsAsync(object entity, string key, IReadOnlyDictionary<string, StoredVector>? known, CancellationToken cancellationToken)
    {
        // A stored vector is valid for its text whichever version of the object it was stored
        // with, so the vectors the object in hand carries are used as they are. The row is read
        // when they are unknown, or when they leave a field without a vector: the embed rule may
        // have stored it since this object was read or saved (it works on its own copy).
        var stored = known ?? entity.GetVectors();
        var read = false;
        async Task<IReadOnlyDictionary<string, StoredVector>> ReadRowAsync()
        {
            read = true;
            var (row, _) = await _tableAdapter.GetAsync(_lottaCatalog.Name, key, cancellationToken: cancellationToken).ConfigureAwait(false);
            return row?.GetVectors() ?? StoredVectors.None;
        }
        stored ??= await ReadRowAsync().ConfigureAwait(false);

        var model = EmbeddingModelId!;
        var vectors = new Dictionary<string, StoredVector>(StringComparer.Ordinal);
        List<(string Field, string Text, string Hash)>? stale = null;
        foreach (var field in VectorFieldsOf(entity.GetType()))
        {
            if (field.Text(entity) is not { Length: > 0 } text)
                continue;
            var hash = StoredVectors.Hash(model, text);
            if (stored.TryGetValue(field.Name, out var vector) && vector.Hash == hash)
                vectors[field.Name] = vector;
            else
                (stale ??= []).Add((field.Name, text, hash));
        }

        if (stale != null && !read)
        {
            var row = await ReadRowAsync().ConfigureAwait(false);
            stale.RemoveAll(s =>
            {
                if (!row.TryGetValue(s.Field, out var vector) || vector.Hash != s.Hash)
                    return false;
                vectors[s.Field] = vector;
                return true;
            });
            if (stale.Count == 0)
                stale = null;
        }

        return new VectorPlan(entity, vectors, stale);
    }

    /// <summary>Whether two objects carry the same vectors (by field and hash).</summary>
    private static bool SameVectors(object a, object b)
    {
        var x = a.GetVectors() ?? StoredVectors.None;
        var y = b.GetVectors() ?? StoredVectors.None;
        return x.Count == y.Count && x.All(v => y.TryGetValue(v.Key, out var w) && w.Hash == v.Value.Hash);
    }

    /// <summary>
    /// Puts on <paramref name="target"/> the vectors of <paramref name="from"/> as well as its own,
    /// those of <paramref name="from"/> first. Always safe: the save keeps only the vectors that
    /// are those of the target's text.
    /// </summary>
    private static void AddVectors(object target, object from)
    {
        var merged = new Dictionary<string, StoredVector>(target.GetVectors() ?? StoredVectors.None, StringComparer.Ordinal);
        foreach (var (field, vector) in from.GetVectors() ?? StoredVectors.None)
            merged[field] = vector;
        target.SetVectors(merged);
    }
}
