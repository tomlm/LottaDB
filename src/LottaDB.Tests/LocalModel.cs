using ElBruno.LocalEmbeddings.Options;

namespace Lotta.Tests;

/// <summary>
/// Locates the ONNX embedding model vendored at <c>/models/bge-micro-v2</c> so the vector
/// search tests never download anything at run time.
///
/// <para>
/// Without this, <c>ElBruno.LocalEmbeddings</c> fetches the model from HuggingFace on first
/// use, which makes the whole suite depend on an external service. CI went red on a
/// <c>429 TooManyRequests</c> from huggingface.co with nothing wrong in the repository.
/// </para>
///
/// <para>
/// When <see cref="LocalEmbeddingsOptions.ModelPath"/> is set, <c>LocalEmbeddingGenerator</c>
/// returns before it constructs a <c>ModelDownloader</c> at all — so setting it is what makes
/// the tests hermetic, not merely faster.
/// </para>
/// </summary>
public static class LocalModel
{
    private const string ModelDirectoryName = "bge-micro-v2";

    private static readonly Lazy<string?> _modelPath = new(FindVendoredModel);

    /// <summary>
    /// Set <c>LOTTA_REQUIRE_LOCAL_MODEL=1</c> to make a missing vendored model a hard error
    /// instead of silently downloading. CI sets this: without it, a broken fetch step would
    /// quietly go back to depending on HuggingFace and keep passing until the next rate limit.
    /// </summary>
    public const string RequireLocalModelVariable = "LOTTA_REQUIRE_LOCAL_MODEL";

    /// <summary>
    /// Build options for the vendored model. Falls back to the download behaviour if the
    /// vendored copy cannot be found, so a fresh checkout still runs rather than hard-failing —
    /// unless <see cref="RequireLocalModelVariable"/> is set.
    /// </summary>
    public static LocalEmbeddingsOptions Options()
    {
        var path = _modelPath.Value;

        if (path == null && Environment.GetEnvironmentVariable(RequireLocalModelVariable) == "1")
        {
            throw new InvalidOperationException(
                $"{RequireLocalModelVariable}=1 but no vendored model was found in any 'models/{ModelDirectoryName}' " +
                $"directory above '{AppContext.BaseDirectory}'. Run scripts/fetch-test-model.sh. " +
                "Refusing to fall back to downloading from HuggingFace — see models/README.md.");
        }

        return new LocalEmbeddingsOptions
        {
            ModelName = "SmartComponents/bge-micro-v2",
            PreferQuantized = true,
            // null => ElBruno.LocalEmbeddings downloads to its own cache, as it did before.
            ModelPath = path,
        };
    }

    /// <summary>
    /// Walk up from the test assembly (<c>src/&lt;project&gt;/bin/&lt;cfg&gt;/&lt;tfm&gt;</c>)
    /// looking for the repository's <c>models/</c> directory. Avoids copying 17MB into the
    /// output of all five test projects just to find it.
    /// </summary>
    private static string? FindVendoredModel()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);

        while (dir != null)
        {
            var candidate = Path.Combine(dir.FullName, "models", ModelDirectoryName);
            // The tokenizer loads from the same directory as the weights, so require both.
            if (File.Exists(Path.Combine(candidate, "model_quantized.onnx")) &&
                File.Exists(Path.Combine(candidate, "tokenizer.json")))
            {
                return candidate;
            }
            dir = dir.Parent;
        }

        return null;
    }
}
