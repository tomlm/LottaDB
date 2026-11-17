using Tiki;

namespace Lotta.Tiki;

/// <summary>
/// Extension methods to integrate Tiki.Net content extraction with LottaDB.
/// </summary>
public static class TikiConfigurationExtensions
{
    /// <summary>
    /// Use Tiki.Net for rich blob metadata extraction on upload.
    /// Replaces the default upload handler with one that parses uploaded blobs
    /// and stores strongly-typed metadata (BlobPhoto, BlobDocument, BlobMusic, etc.).
    /// </summary>
    /// <param name="config">The LottaDB configuration.</param>
    /// <param name="engine">Optional pre-configured TikiEngine. If null, a default engine is created.</param>
    /// <returns>The configuration for chaining.</returns>
    public static ILottaConfiguration UseTikiExtraction(
        this ILottaConfiguration config,
        TikiEngine? engine = null)
    {
        engine ??= new TikiEngine();

        config.OnUpload(async (path, contentType, stream, db, cancellationToken) =>
        {
            // The name lets Tiki tell apart formats whose bytes look alike (CSV, JSON, DOCX); content that
            // disagrees with the name still wins.
            var tikiFile = await engine.ParseAsync(stream, Path.GetFileName(path), cancellationToken);
            return BlobFileMapper.FromTikiFile(tikiFile, path);
        });

        return config;
    }
}
