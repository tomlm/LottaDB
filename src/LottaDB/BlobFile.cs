using System.Text.Json.Serialization;
using Lucene.Net.Linq.Mapping;

namespace Lotta;

/// <summary>
/// Base metadata entity for a blob stored in LottaDB.
/// Automatically created by the OnUpload handler when a blob is uploaded.
/// Key = blob path.
/// </summary>
[DefaultSearch(nameof(Content))]
public class BlobFile
{
    /// <summary>The owning database. Set automatically when loaded or saved.</summary>
    [JsonIgnore]
    internal LottaDB? Database { get; set; }

    /// <summary>The blob path (relative to this database).</summary>
    [Key]
    public string Path { get; set; } = string.Empty;

    /// <summary>The file name (extracted from the path).</summary>
    [Queryable]
    public string? Name { get; set; }

    /// <summary>Path to the folder containing the file.</summary>
    [Queryable]
    public string? FolderPath { get; set; }

    /// <summary>The detected media type (e.g. "image/jpeg", "application/pdf").</summary>
    [Queryable(QueryableMode.NotAnalyzed)]
    public string? MediaType { get; set; }

    /// <summary>The document title.</summary>
    [Queryable]
    public string? Title { get; set; }

    /// <summary>Authors of the file.</summary>
    public string[]? Authors { get; set; }

    /// <summary>A description or summary.</summary>
    [Queryable]
    public string? Description { get; set; }

    /// <summary>Date the file was originally created.</summary>
    [Queryable]
    public DateTime? DateCreated { get; set; }

    /// <summary>Date the file was last modified.</summary>
    [Queryable]
    public DateTime? DateModified { get; set; }

    /// <summary>File size in bytes.</summary>
    [Queryable]
    public long? ContentLength { get; set; }

    /// <summary>Keywords or tags associated with the file.</summary>
    public string[]? Keywords { get; set; }

    /// <summary>
    /// Extracted text content. Indexed in Lucene for full-text search
    /// but not promoted to a Table Storage column (may be very large).
    /// </summary>
    [Field(IndexMode.Analyzed)]
    public string? Content { get; set; }

    /// <summary>Download the blob content as a stream.</summary>
    /// <returns>The blob content, or null if the blob no longer exists.</returns>
    public Task<Stream?> DownloadAsync(CancellationToken cancellationToken = default)
    {
        var db = Database ?? throw new InvalidOperationException("BlobFile is not associated with a database. Load it via GetAsync or Query first.");
        return db.DownloadBlobAsync(Path, cancellationToken);
    }

    /// <summary>Delete this blob and its metadata.</summary>
    /// <returns>True if the blob was deleted, false if it didn't exist.</returns>
    public Task<bool> DeleteAsync(CancellationToken cancellationToken = default)
    {
        var db = Database ?? throw new InvalidOperationException("BlobFile is not associated with a database. Load it via GetAsync or Query first.");
        return db.DeleteBlobAsync(Path, cancellationToken);
    }
}

/// <summary>Metadata for image files (JPEG, PNG, TIFF, etc.).</summary>
public class BlobPhoto : BlobFile
{
    [Queryable]
    public string? CameraManufacturer { get; set; }

    [Queryable]
    public string? CameraModel { get; set; }

    [Queryable]
    public string? LensModel { get; set; }

    [Queryable]
    public DateTime? DateTaken { get; set; }

    [Queryable]
    public double? ExposureTime { get; set; }

    [Queryable]
    public double? FNumber { get; set; }

    [Queryable]
    public int? IsoSpeed { get; set; }

    [Queryable]
    public double? FocalLength { get; set; }

    [Queryable]
    public int? Orientation { get; set; }

    [Queryable]
    public int? Width { get; set; }

    [Queryable]
    public int? Height { get; set; }

    [Queryable]
    public double? Latitude { get; set; }

    [Queryable]
    public double? Longitude { get; set; }

    [Queryable]
    public string? Flash { get; set; }

    [Queryable(QueryableMode.NotAnalyzed)]
    public string? WhiteBalance { get; set; }

    [Queryable(QueryableMode.NotAnalyzed)]
    public string? MeteringMode { get; set; }

    [Queryable]
    public double? ExposureBias { get; set; }
}

/// <summary>Base for office documents (word processing, spreadsheets, presentations).</summary>
public class BlobOfficeDocument : BlobFile
{
    [Queryable]
    public string? LastAuthor { get; set; }

    [Queryable]
    public string? Company { get; set; }

    [Queryable]
    public string? Manager { get; set; }

    [Queryable]
    public string? Subject { get; set; }

    [Queryable]
    public string? Category { get; set; }

    [Queryable]
    public string? ApplicationName { get; set; }

    [Queryable]
    public int? RevisionNumber { get; set; }
}

/// <summary>Metadata for word processing documents (PDF, DOCX, DOC, RTF).</summary>
public class BlobDocument : BlobOfficeDocument
{
    [Queryable]
    public int? PageCount { get; set; }

    [Queryable]
    public int? WordCount { get; set; }

    [Queryable]
    public int? CharacterCount { get; set; }
}

/// <summary>Metadata for spreadsheet documents (XLSX, XLS).</summary>
public class BlobSpreadsheet : BlobOfficeDocument
{
    [Queryable]
    public int? SheetCount { get; set; }

    public string[]? SheetNames { get; set; }
}

/// <summary>Metadata for presentation documents (PPTX, PPT).</summary>
public class BlobPresentation : BlobOfficeDocument
{
    [Queryable]
    public int? SlideCount { get; set; }
}

/// <summary>Base for media files (audio and video).</summary>
public class BlobMedia : BlobFile
{
    [Queryable]
    public double? DurationSeconds { get; set; }

    [Queryable]
    public int? Bitrate { get; set; }

    [Queryable]
    public int? SampleRate { get; set; }

    [Queryable]
    public int? Channels { get; set; }

    [Queryable(QueryableMode.NotAnalyzed)]
    public string? Codec { get; set; }
}

/// <summary>Metadata for music files with artist/album/track tags.</summary>
public class BlobMusic : BlobMedia
{
    [Queryable]
    public string? Artist { get; set; }

    [Queryable]
    public string? AlbumArtist { get; set; }

    [Queryable]
    public string? Album { get; set; }

    public string[]? Genre { get; set; }

    [Queryable]
    public int? TrackNumber { get; set; }

    [Queryable]
    public int? DiscNumber { get; set; }

    [Queryable]
    public int? Year { get; set; }

    [Queryable]
    public string? Composer { get; set; }
}

/// <summary>Metadata for video files (MP4, AVI, MKV, MOV, etc.).</summary>
public class BlobVideo : BlobMedia
{
    [Queryable]
    public int? Width { get; set; }

    [Queryable]
    public int? Height { get; set; }

    [Queryable]
    public double? FrameRate { get; set; }

    [Queryable]
    public int? VideoBitrate { get; set; }

    [Queryable]
    public string? VideoCodec { get; set; }
}

/// <summary>Metadata for email messages (EML, MSG).</summary>
public class BlobMessage : BlobFile
{
    [Queryable(QueryableMode.NotAnalyzed)]
    public string? FromAddress { get; set; }

    [Queryable]
    public string? FromName { get; set; }

    public string[]? ToAddresses { get; set; }

    [Queryable]
    public string? Subject { get; set; }

    [Queryable]
    public DateTime? DateSent { get; set; }

    [Queryable]
    public DateTime? DateReceived { get; set; }

    public string[]? AttachmentNames { get; set; }

    [Queryable]
    public string? ConversationId { get; set; }
}

/// <summary>Metadata for HTML web pages.</summary>
public class BlobWebPage : BlobFile
{
    [Queryable]
    public string? Language { get; set; }

    [Queryable]
    public string? Generator { get; set; }

    [Queryable(QueryableMode.NotAnalyzed)]
    public string? Charset { get; set; }

    public string[]? Links { get; set; }
}
