namespace Lotta;

/// <summary>
/// Built-in default blob upload handler. Creates a BlobFile with basic metadata
/// derived from the blob path and content. For known text formats, extracts the
/// text into the Content property for full-text search.
/// </summary>
internal static class DefaultBlobHandler
{
    private static readonly HashSet<string> TextExtensions = new(StringComparer.OrdinalIgnoreCase)
    {
        ".txt", ".md", ".markdown", ".csv", ".tsv", ".log",
        ".json", ".xml", ".yaml", ".yml", ".toml", ".ini", ".cfg",
        ".html", ".htm", ".css", ".svg",
        ".cs", ".js", ".ts", ".py", ".rb", ".java", ".go", ".rs", ".c", ".cpp", ".h", ".hpp",
        ".sh", ".bash", ".ps1", ".bat", ".cmd",
        ".sql", ".graphql", ".gql",
        ".env", ".gitignore", ".dockerignore", ".editorconfig",
    };

    private static readonly Dictionary<string, string> MimeTypes = new(StringComparer.OrdinalIgnoreCase)
    {
        // Text
        [".txt"] = "text/plain",
        [".md"] = "text/markdown",
        [".markdown"] = "text/markdown",
        [".csv"] = "text/csv",
        [".tsv"] = "text/tab-separated-values",
        [".log"] = "text/plain",
        // Data
        [".json"] = "application/json",
        [".xml"] = "application/xml",
        [".yaml"] = "application/yaml",
        [".yml"] = "application/yaml",
        [".toml"] = "application/toml",
        [".ini"] = "text/plain",
        [".cfg"] = "text/plain",
        // Web
        [".html"] = "text/html",
        [".htm"] = "text/html",
        [".css"] = "text/css",
        [".svg"] = "image/svg+xml",
        // Code
        [".cs"] = "text/x-csharp",
        [".js"] = "text/javascript",
        [".ts"] = "text/typescript",
        [".py"] = "text/x-python",
        [".rb"] = "text/x-ruby",
        [".java"] = "text/x-java",
        [".go"] = "text/x-go",
        [".rs"] = "text/x-rust",
        [".c"] = "text/x-c",
        [".cpp"] = "text/x-c++",
        [".h"] = "text/x-c",
        [".hpp"] = "text/x-c++",
        [".sh"] = "text/x-shellscript",
        [".bash"] = "text/x-shellscript",
        [".ps1"] = "text/x-powershell",
        [".bat"] = "text/x-bat",
        [".cmd"] = "text/x-bat",
        [".sql"] = "text/x-sql",
        [".graphql"] = "text/x-graphql",
        [".gql"] = "text/x-graphql",
        // Images
        [".jpg"] = "image/jpeg",
        [".jpeg"] = "image/jpeg",
        [".png"] = "image/png",
        [".gif"] = "image/gif",
        [".bmp"] = "image/bmp",
        [".webp"] = "image/webp",
        [".tiff"] = "image/tiff",
        [".tif"] = "image/tiff",
        [".ico"] = "image/x-icon",
        // Audio
        [".mp3"] = "audio/mpeg",
        [".wav"] = "audio/wav",
        [".flac"] = "audio/flac",
        [".ogg"] = "audio/ogg",
        [".aac"] = "audio/aac",
        [".wma"] = "audio/x-ms-wma",
        [".m4a"] = "audio/mp4",
        // Video
        [".mp4"] = "video/mp4",
        [".avi"] = "video/x-msvideo",
        [".mkv"] = "video/x-matroska",
        [".mov"] = "video/quicktime",
        [".wmv"] = "video/x-ms-wmv",
        [".webm"] = "video/webm",
        [".flv"] = "video/x-flv",
        // Documents
        [".pdf"] = "application/pdf",
        [".doc"] = "application/msword",
        [".docx"] = "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        [".xls"] = "application/vnd.ms-excel",
        [".xlsx"] = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        [".ppt"] = "application/vnd.ms-powerpoint",
        [".pptx"] = "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        [".rtf"] = "application/rtf",
        [".odt"] = "application/vnd.oasis.opendocument.text",
        [".ods"] = "application/vnd.oasis.opendocument.spreadsheet",
        [".odp"] = "application/vnd.oasis.opendocument.presentation",
        // Email
        [".eml"] = "message/rfc822",
        [".msg"] = "application/vnd.ms-outlook",
        // Archives
        [".zip"] = "application/zip",
        [".gz"] = "application/gzip",
        [".tar"] = "application/x-tar",
        [".7z"] = "application/x-7z-compressed",
        [".rar"] = "application/x-rar-compressed",
    };

    internal static async Task<BlobFile?> HandleAsync(string path, string? contentType, Stream stream, BlobFile? existing, LottaDB db)
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
        if (IsTextExtension(ext))
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

        return MimeTypes.TryGetValue(extension, out var mime) ? mime : "application/octet-stream";
    }

    internal static bool IsTextExtension(string extension)
    {
        return !string.IsNullOrEmpty(extension) && TextExtensions.Contains(extension);
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
