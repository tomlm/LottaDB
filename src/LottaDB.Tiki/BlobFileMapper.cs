using Tiki.Documents;

namespace Lotta.Tiki;

/// <summary>
/// Maps Tiki.Net parsed results (TikiFile hierarchy) to LottaDB BlobFile entities.
/// </summary>
public static class BlobFileMapper
{
    /// <summary>
    /// Convert a TikiFile parse result to the corresponding BlobFile entity.
    /// </summary>
    /// <param name="tikiFile">The parsed Tiki result.</param>
    /// <param name="blobPath">The blob path to use as the entity key.</param>
    /// <returns>A BlobFile (or subclass) populated with extracted metadata.</returns>
    public static BlobFile FromTikiFile(TikiFile tikiFile, string blobPath)
    {
        BlobFile result = tikiFile switch
        {
            TikiPhoto p => new BlobPhoto
            {
                CameraManufacturer = p.CameraManufacturer,
                CameraModel = p.CameraModel,
                LensModel = p.LensModel,
                DateTaken = p.DateTaken,
                ExposureTime = p.ExposureTime,
                FNumber = p.FNumber,
                IsoSpeed = p.IsoSpeed,
                FocalLength = p.FocalLength,
                Orientation = p.Orientation,
                Width = p.Width,
                Height = p.Height,
                Latitude = p.Latitude,
                Longitude = p.Longitude,
                Flash = p.Flash,
                WhiteBalance = p.WhiteBalance,
                MeteringMode = p.MeteringMode,
                ExposureBias = p.ExposureBias,
            },
            TikiMusic m => new BlobMusic
            {
                Artist = m.Artist,
                AlbumArtist = m.AlbumArtist,
                Album = m.Album,
                Genre = m.Genre,
                TrackNumber = m.TrackNumber,
                DiscNumber = m.DiscNumber,
                Year = m.Year,
                Composer = m.Composer,
                DurationSeconds = m.Duration?.TotalSeconds,
                Bitrate = m.Bitrate,
                SampleRate = m.SampleRate,
                Channels = m.Channels,
                Codec = m.Codec,
            },
            TikiVideo v => new BlobVideo
            {
                Width = v.Width,
                Height = v.Height,
                FrameRate = v.FrameRate,
                VideoBitrate = v.VideoBitrate,
                VideoCodec = v.VideoCodec,
                DurationSeconds = v.Duration?.TotalSeconds,
                Bitrate = v.Bitrate,
                SampleRate = v.SampleRate,
                Channels = v.Channels,
                Codec = v.Codec,
            },
            TikiDocument d => new BlobDocument
            {
                PageCount = d.PageCount,
                WordCount = d.WordCount,
                CharacterCount = d.CharacterCount,
                LastAuthor = d.LastAuthor,
                Company = d.Company,
                Manager = d.Manager,
                Subject = d.Subject,
                Category = d.Category,
                ApplicationName = d.ApplicationName,
                RevisionNumber = d.RevisionNumber,
            },
            TikiSpreadsheet s => new BlobSpreadsheet
            {
                SheetCount = s.SheetCount,
                SheetNames = s.SheetNames,
                LastAuthor = s.LastAuthor,
                Company = s.Company,
                Manager = s.Manager,
                Subject = s.Subject,
                Category = s.Category,
                ApplicationName = s.ApplicationName,
                RevisionNumber = s.RevisionNumber,
            },
            TikiPresentation pr => new BlobPresentation
            {
                SlideCount = pr.SlideCount,
                LastAuthor = pr.LastAuthor,
                Company = pr.Company,
                Manager = pr.Manager,
                Subject = pr.Subject,
                Category = pr.Category,
                ApplicationName = pr.ApplicationName,
                RevisionNumber = pr.RevisionNumber,
            },
            TikiMessage msg => new BlobMessage
            {
                FromAddress = msg.FromAddress,
                FromName = msg.FromName,
                ToAddresses = msg.ToAddresses,
                Subject = msg.Subject,
                DateSent = msg.DateSent,
                DateReceived = msg.DateReceived,
                AttachmentNames = msg.AttachmentNames,
                ConversationId = msg.ConversationId,
            },
            TikiWebPage wp => new BlobWebPage
            {
                Language = wp.Language,
                Generator = wp.Generator,
                Charset = wp.Charset,
                Links = wp.Links,
            },
            _ => new BlobFile()
        };

        // Copy base TikiFile properties — derive Name/FolderPath from blob path
        // since TikiEngine doesn't set them when parsing from a stream.
        result.Path = blobPath;
        result.Name = Path.GetFileName(blobPath);
        var folder = Path.GetDirectoryName(blobPath)?.Replace('\\', '/');
        result.FolderPath = string.IsNullOrEmpty(folder) ? null : folder;
        result.MediaType = tikiFile.MediaType.ToString();
        result.Title = tikiFile.Title;
        result.Authors = tikiFile.Authors;
        result.Description = tikiFile.Description;
        result.DateCreated = tikiFile.DateCreated;
        result.DateModified = tikiFile.DateModified;
        result.ContentLength = tikiFile.ContentLength;
        result.Keywords = tikiFile.Keywords;
        result.Content = string.IsNullOrEmpty(tikiFile.Content) ? null : tikiFile.Content;

        return result;
    }
}
