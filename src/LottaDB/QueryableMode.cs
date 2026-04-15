namespace Lotta;

/// <summary>
/// Controls how a <see cref="QueryableAttribute"/> property is indexed in Lucene.
/// </summary>
public enum QueryableMode
{
    /// <summary>
    /// Smart default: strings are analyzed (full-text searchable),
    /// value types are not analyzed (exact match only).
    /// </summary>
    Auto,

    /// <summary>Tokenized and searchable as full text.</summary>
    Analyzed,

    /// <summary>Indexed as-is for exact match filtering only.</summary>
    NotAnalyzed
}
