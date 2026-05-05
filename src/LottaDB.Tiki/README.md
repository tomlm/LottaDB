[![Build and Test](https://github.com/tomlm/LottaDB/actions/workflows/BuildAndRunTests.yml/badge.svg)](https://github.com/tomlm/LottaDB/actions/workflows/BuildAndRunTests.yml)
[![NuGet](https://img.shields.io/nuget/v/LottaDB.Tiki.svg)](https://www.nuget.org/packages/LottaDB.Tiki)

![Logo](https://raw.githubusercontent.com/tomlm/LottaDB/refs/heads/main/icon.png)

# LottaDB.Tiki

Rich blob metadata extraction for LottaDB, powered by [Tiki.Net](https://github.com/tomlm/Tiki.Net).

When you upload a blob, Tiki.Net automatically parses it and stores strongly-typed metadata (EXIF from photos, ID3 from music, page counts from PDFs, etc.) as queryable entities in LottaDB.

## Installation

```bash
dotnet add package LottaDB.Tiki
```

## Quick Start

```csharp
using Lotta;
using Lotta.Tiki;

var catalog = new LottaCatalog("myapp", connectionString);
var db = await catalog.GetDatabaseAsync("media", config =>
{
    config.UseTikiExtraction(); // one line to enable
});

// Upload a photo — metadata is extracted automatically
await using var stream = File.OpenRead("vacation.jpg");
var photo = (BlobPhoto) await db.UploadBlobAsync("photos/vacation.jpg", stream);

// Rich metadata is available immediately
Console.WriteLine($"Camera: {photo.CameraModel}");
Console.WriteLine($"ISO: {photo.IsoSpeed}");
Console.WriteLine($"Size: {photo.Width}x{photo.Height}");
```

## How It Works

`UseTikiExtraction()` registers an `OnUpload` handler that:

1. Receives the blob content as a concurrent stream (zero-copy via TeeStream)
2. Parses it with Tiki.Net's auto-detecting parser
3. Maps the result to the correct `BlobFile` subtype
4. Saves the metadata entity — queryable and full-text searchable

The blob upload and parsing happen concurrently — no buffering the entire file in memory.

## Querying Metadata

All metadata properties are queryable via LINQ and Lucene:

```csharp
// Find high-ISO photos
var nightPhotos = db.Search<BlobPhoto>(p => p.IsoSpeed > 3200).ToList();

// Find large PDFs
var bigDocs = db.Search<BlobDocument>(p => p.PageCount > 50).ToList();

// Find music by artist
var songs = db.Search<BlobMusic>(m => m.Artist == "Pink Floyd").ToList();

// Full-text search across extracted document content
var results = db.Search<BlobFile>("quarterly revenue").ToList();

// Polymorphic — returns all file types
var allFiles = db.Search<BlobFile>().ToList();
```

## Supported File Types

| File Type | BlobFile Subtype | Extracted Metadata |
|-----------|-----------------|-------------------|
| JPEG, PNG, TIFF, etc. | `BlobPhoto` | Camera, ISO, aperture, focal length, GPS, dimensions |
| PDF, DOCX, DOC, RTF | `BlobDocument` | Page count, word count, author, company, text content |
| XLSX, XLS | `BlobSpreadsheet` | Sheet count, sheet names |
| PPTX, PPT | `BlobPresentation` | Slide count |
| MP3, FLAC, WAV, OGG | `BlobMusic` | Artist, album, track, year, genre, duration |
| MP4, AVI, MKV, MOV | `BlobVideo` | Resolution, frame rate, codec, duration |
| EML | `BlobMessage` | From, to, subject, date sent |
| HTML | `BlobWebPage` | Language, charset, links |

Text content is automatically extracted and indexed for full-text search.

## Configuration

```csharp
// Default — uses auto-configured TikiEngine
config.UseTikiExtraction();

// Custom engine with specific parsers
var engine = new TikiEngine(TikiConfig.CreateBuilder()
    .AddParser(new PdfParser())
    .AddParser(new ImageParser())
    .Build());
config.UseTikiExtraction(engine);
```

## BlobFile Convenience Methods

```csharp
var file = await db.GetAsync<BlobPhoto>("photos/vacation.jpg");

// Download the blob content
Stream? stream = await file.DownloadAsync();

// Delete blob + metadata in one call
await file.DeleteAsync();
```

## Without Tiki (Default Handler)

LottaDB includes a built-in default handler that doesn't require Tiki.Net:

```csharp
config.OnUpload(); // uses file extension for type detection
```

This gives you:
- File extension to MIME type mapping
- Correct `BlobFile` subtype (BlobPhoto for .jpg, BlobDocument for .pdf, etc.)
- Text content extraction for known text formats (.txt, .md, .json, .cs, .py, etc.)
- Basic metadata (path, name, folder, media type, content length)

The Tiki handler adds deep content parsing (EXIF, ID3 tags, page counts, etc.) on top.
