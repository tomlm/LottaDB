using MimeMapping;

namespace Lotta;

/// <summary>
/// Built-in default blob upload handler. Creates a BlobFile with basic metadata
/// derived from the blob path and content. For known text formats, extracts the
/// text into the Content property for full-text search.
/// </summary>
internal static class DefaultBlobHandler
{

    internal static async Task<BlobFile?> HandleAsync(string path, string? contentType, Stream stream, LottaDB db)
    {
        var ext = Path.GetExtension(path);
        var mimeType = contentType ?? GetMimeType(ext);
        var blobFile = CreateBlobFileForMimeType(mimeType);

        blobFile.Path = path;
        blobFile.Name = Path.GetFileName(path);
        var folder = Path.GetDirectoryName(path)?.Replace('\\', '/');
        blobFile.FolderPath = string.IsNullOrEmpty(folder) ? null : folder;
        blobFile.MediaType = mimeType;

        // For text formats, read the content
        if (IsTextContent(mimeType, ext))
        {
            using var reader = new StreamReader(stream, leaveOpen: true);
            blobFile.Content = await reader.ReadToEndAsync();
            blobFile.ContentLength = stream.CanSeek ? stream.Length : System.Text.Encoding.UTF8.GetByteCount(blobFile.Content);
        }
        else
        {
            // For binary formats, just record the length if the stream supports it
            if (stream.CanSeek)
                blobFile.ContentLength = stream.Length;
        }

        return blobFile;
    }

    internal static string GetMimeType(string extension)
    {
        if (string.IsNullOrEmpty(extension))
            return "application/octet-stream";

        return MimeUtility.GetMimeMapping(extension);
    }

    /// <summary>
    /// Well-known source code and config extensions that MimeMapping doesn't classify as text.
    /// </summary>
    private static readonly HashSet<string> TextFileExtensions = new(StringComparer.OrdinalIgnoreCase)
    {
        // Source code
        ".cs", ".fs", ".vb",                           // .NET
        ".py", ".pyw",                                  // Python
        ".rb", ".rake",                                 // Ruby
        ".go",                                          // Go
        ".rs",                                          // Rust
        ".swift",                                       // Swift
        ".kt", ".kts",                                  // Kotlin
        ".scala",                                       // Scala
        ".java",                                        // Java
        ".c", ".cpp", ".cc", ".cxx", ".h", ".hpp",     // C/C++
        ".m", ".mm",                                    // Objective-C
        ".ts", ".tsx", ".jsx", ".mjs", ".cjs",         // TypeScript/JS variants
        ".lua",                                         // Lua
        ".r",                                           // R
        ".jl",                                          // Julia
        ".ex", ".exs",                                  // Elixir
        ".erl", ".hrl",                                 // Erlang
        ".hs", ".lhs",                                  // Haskell
        ".clj", ".cljs",                                // Clojure
        ".dart",                                        // Dart
        ".php",                                         // PHP
        ".pl", ".pm",                                   // Perl
        ".groovy",                                      // Groovy
        // Shell/scripting
        ".sh", ".bash", ".zsh", ".fish",
        ".ps1", ".psm1", ".psd1",                       // PowerShell
        ".bat", ".cmd",                                 // Windows batch
        // Query languages
        ".sql", ".graphql", ".gql",
        // Markup/config
        ".toml", ".ini", ".cfg", ".conf",
        ".env", ".properties",
        ".tf", ".hcl",                                  // Terraform
        ".proto",                                       // Protocol Buffers
        // Build/project files
        ".csproj", ".fsproj", ".vbproj", ".sln", ".slnx",
        ".gradle",
        ".cmake",
        ".makefile",
        // Misc text
        ".log", ".diff", ".patch",
        ".gitignore", ".gitattributes", ".dockerignore", ".editorconfig",
        ".yml",                                         // MimeMapping knows .yaml but not .yml
    };

    internal static bool IsTextContent(string mimeType, string extension)
    {
        return mimeType.StartsWith("text/")
            || mimeType is "application/json" or "application/xml"
               or "application/yaml" or "application/toml"
               or "image/svg+xml"
            || TextFileExtensions.Contains(extension);
    }

    private static BlobFile CreateBlobFileForMimeType(string mimeType)
    {
        // Map mime type prefix to the appropriate BlobFile subtype
        if (mimeType.StartsWith("image/"))
            return new BlobPhoto();
        if (mimeType.StartsWith("audio/"))
            return new BlobMusic();
        if (mimeType.StartsWith("video/"))
            return new BlobVideo();
        if (mimeType.StartsWith("message/") || mimeType == "application/vnd.ms-outlook")
            return new BlobMessage();

        return mimeType switch
        {
            "application/pdf" or
            "application/msword" or
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document" or
            "application/rtf" or
            "application/vnd.oasis.opendocument.text"
                => new BlobDocument(),

            "application/vnd.ms-excel" or
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" or
            "application/vnd.oasis.opendocument.spreadsheet"
                => new BlobSpreadsheet(),

            "application/vnd.ms-powerpoint" or
            "application/vnd.openxmlformats-officedocument.presentationml.presentation" or
            "application/vnd.oasis.opendocument.presentation"
                => new BlobPresentation(),

            "text/html" => new BlobWebPage(),

            _ => new BlobFile()
        };
    }
}
