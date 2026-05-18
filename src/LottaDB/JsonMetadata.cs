using System.Text.Json;

namespace Lotta;

/// <summary>
/// Defines a schema for dynamic (JSON Schema-defined) documents.
/// Only queryable properties are declared — the full JSON document is stored and retrieved as-is.
/// </summary>
public class JsonMetadata
{
    /// <summary>Returns true if the given stored type name is a JSON document type.</summary>
    internal static bool IsJsonTypeName(string? typeName) =>
        typeName == typeof(System.Text.Json.JsonDocument).FullName;

    /// <summary>A unique name for this document type (e.g. "Person"). Used as the type discriminator in storage and search.</summary>
    public string TypeName { get; set; } = null!;

    /// <summary>The JSON property name used as the document key. Defaults to "Id".</summary>
    public string KeyProperty { get; set; } = "Id";

    /// <summary>Key generation strategy. <see cref="Lotta.KeyMode.Auto"/> generates a ULID when the key is missing; <see cref="Lotta.KeyMode.Manual"/> requires the caller to supply it.</summary>
    public KeyMode KeyMode { get; set; } = KeyMode.Auto;

    /// <summary>The name of the default search property, or null to use the auto-generated _content_ field.</summary>
    public string? DefaultSearchProperty { get; set; }

    /// <summary>The queryable properties defined by this schema. These are indexed in Lucene and promoted to Table Storage columns.</summary>
    public List<IndexedJsonProperty> Properties { get; set; } = new();

    /// <summary>When true, auto-discover and index all top-level simple-type properties from documents.</summary>
    public bool AutoQueryable { get; set; }

    /// <summary>Convention-based key property names for auto-detecting document keys. Set from database config.</summary>
    public string[]? AutoKeyProperties { get; set; }

    /// <summary>Optional discriminator expression for auto-classifying documents (e.g. "$.type == 'Person'").</summary>
    public string? Match { get; set; }

    /// <summary>Parse from a <see cref="JsonDocumentType"/> entity.</summary>
    public static JsonMetadata Parse(JsonDocumentType docType)
    {
        var result = new JsonMetadata
        {
            TypeName = docType.Name,
            KeyProperty = docType.Key ?? "Id",
            KeyMode = docType.KeyMode,
            AutoQueryable = docType.AutoQueryable,
            Match = docType.Match,
        };

        foreach (var prop in docType.Properties)
        {
            var clrType = ResolveClrTypeFromString(prop.Type);
            var isAnalyzed = prop.Mode switch
            {
                QueryableMode.Analyzed => true,
                QueryableMode.NotAnalyzed => false,
                _ => clrType == typeof(string) // Auto: strings analyzed, others not
            };
            var jsonPath = prop.JsonPath != prop.Name ? prop.JsonPath : null;
            result.Properties.Add(new IndexedJsonProperty(prop.Name, clrType, isAnalyzed, jsonPath, prop.Vector));

            if (prop.DefaultSearch)
                result.DefaultSearchProperty = prop.Name;
        }

        return result;
    }

    private static Type ResolveClrTypeFromString(string? type) => type switch
    {
        "string" => typeof(string),
        "integer" => typeof(int),
        "number" => typeof(double),
        "boolean" => typeof(bool),
        _ => typeof(string)
    };

    public static JsonMetadata Parse(string typeName, JsonElement schema)
    {
        var result = new JsonMetadata { TypeName = typeName };

        if (schema.TryGetProperty("key", out var keyProp))
            result.KeyProperty = keyProp.GetString() ?? "Id";

        if (schema.TryGetProperty("keyMode", out var keyModeProp))
        {
            var modeStr = keyModeProp.GetString();
            if (Enum.TryParse<KeyMode>(modeStr, ignoreCase: true, out var mode))
                result.KeyMode = mode;
        }

        if (schema.TryGetProperty("properties", out var props) && props.ValueKind == JsonValueKind.Object)
        {
            foreach (var prop in props.EnumerateObject())
            {
                var clrType = ResolveClrType(prop.Value);
                var isAnalyzed = clrType == typeof(string);
                result.Properties.Add(new IndexedJsonProperty(prop.Name, clrType, isAnalyzed));
            }
        }

        return result;
    }

    /// <summary>Default convention-based key property names.</summary>
    internal static readonly string[] DefaultAutoKeyProperties =
        ["id", "_id", "key", "_key", "pk", "primarykey", "uuid", "guid"];

    /// <summary>
    /// Extract the key value from a JSON document, or generate a ULID for Auto mode.
    /// When KeyProperty is the default ("Id"), also tries convention-based detection.
    /// </summary>
    public string GetKey(JsonElement json)
    {
        // Support both property name ("Id") and JsonPath ("$.user.id")
        JsonElement? keyVal = KeyProperty.Contains('.')
            ? NavigatePath(json, KeyProperty)
            : json.TryGetProperty(KeyProperty, out var v) ? v : null;

        if (keyVal != null)
        {
            var key = keyVal.Value.ToString();
            if (!string.IsNullOrEmpty(key))
                return key;
        }

        // Convention-based key detection when using the default "Id" key property
        if (KeyProperty == "Id")
        {
            var conventionKey = DetectKeyFromDocument(json, AutoKeyProperties);
            if (conventionKey != null)
                return conventionKey;
        }

        if (KeyMode == KeyMode.Auto)
            return Ulid.NewUlid().ToString();

        throw new InvalidOperationException(
            $"Key property '{KeyProperty}' is missing or empty in JSON document for schema '{TypeName}' with Manual key mode.");
    }

    /// <summary>
    /// Detect a key value from a JSON document using common convention names.
    /// Returns null if no convention match is found.
    /// </summary>
    internal static string? DetectKeyFromDocument(JsonElement root, string[]? autoKeyProperties = null)
    {
        foreach (var convention in autoKeyProperties ?? DefaultAutoKeyProperties)
        {
            foreach (var prop in root.EnumerateObject())
            {
                if (prop.Name.Equals(convention, StringComparison.OrdinalIgnoreCase))
                {
                    var val = prop.Value;
                    if (val.ValueKind == JsonValueKind.String)
                    {
                        var s = val.GetString();
                        if (!string.IsNullOrEmpty(s)) return s;
                    }
                    else if (val.ValueKind == JsonValueKind.Number)
                        return val.GetRawText();
                    break;
                }
            }
        }
        return null;
    }

    private static JsonElement? NavigatePath(JsonElement root, string path)
    {
        var current = root;
        var segments = path.TrimStart('$', '.').Split('.');
        foreach (var segment in segments)
        {
            if (!current.TryGetProperty(segment, out var next))
                return null;
            current = next;
        }
        return current;
    }

    /// <summary>Extract the key from a JsonDocument.</summary>
    public string GetKey(JsonDocument json) => GetKey(json.RootElement);

    /// <summary>
    /// Computes a deterministic hash of a JsonMetadata's property definitions.
    /// Used to detect changes that require a Lucene reindex.
    /// </summary>
    public static string ComputeHash(JsonMetadata schema)
    {
        var data = new
        {
            schema.TypeName,
            schema.KeyProperty,
            schema.DefaultSearchProperty,
            KeyMode = schema.KeyMode.ToString(),
            Properties = schema.Properties
                .OrderBy(p => p.Name)
                .Select(p => new
                {
                    p.Name,
                    Type = p.ClrType.FullName,
                    p.IsAnalyzed,
                    p.JsonPath,
                    p.IsVectorField
                })
        };
        var json = JsonSerializer.Serialize(data, new JsonSerializerOptions { WriteIndented = false });
        var hash = System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(json));
        return Convert.ToHexString(hash);
    }

    private static Type ResolveClrType(JsonElement propDef)
    {
        if (propDef.TryGetProperty("type", out var typeProp))
        {
            return typeProp.GetString() switch
            {
                "string" => typeof(string),
                "integer" => typeof(int),
                "number" => typeof(double),
                "boolean" => typeof(bool),
                _ => typeof(string) // fallback
            };
        }

        return typeof(string); // default
    }

    /// <summary>
    /// Extract a value from a JSON element using a <see cref="IndexedJsonProperty"/>.
    /// If the property has a JsonPath, navigates the dot-separated path (e.g. "$.address.city").
    /// Otherwise, does a simple top-level property lookup by name.
    /// </summary>
    internal static JsonElement? GetValue(JsonElement root, IndexedJsonProperty prop)
    {
        if (prop.JsonPath == null)
            return root.TryGetProperty(prop.Name, out var val) ? val : null;

        // Dot-path navigation: "$.address.city" → root.address.city
        var current = root;
        var segments = prop.JsonPath.TrimStart('$', '.').Split('.');
        foreach (var segment in segments)
        {
            if (!current.TryGetProperty(segment, out var next))
                return null;
            current = next;
        }
        return current;
    }

    /// <summary>
    /// Evaluate whether a JSON document matches this schema's match expression.
    /// Returns true if the discriminator matches or if no discriminator is set.
    /// Supports simple equality expressions like <c>$.type == 'Person'</c>.
    /// </summary>
    public bool MatchesDocument(JsonElement root)
    {
        if (string.IsNullOrEmpty(Match)) return false;

        // Parse discriminator: "$.path == 'value'" or "$.path != 'value'"
        var disc = Match.AsSpan().Trim();
        var eqIndex = disc.IndexOf("==");
        var neqIndex = disc.IndexOf("!=");
        bool isNegated = false;
        int opIndex;

        if (neqIndex >= 0)
        {
            opIndex = neqIndex;
            isNegated = true;
        }
        else if (eqIndex >= 0)
        {
            opIndex = eqIndex;
        }
        else
        {
            return false; // unsupported operator
        }

        var pathPart = disc[..opIndex].Trim().ToString();
        var valuePart = disc[(opIndex + 2)..].Trim().ToString().Trim('\'', '"');

        // Navigate JSON path
        var element = NavigatePath(root, pathPart);
        if (element == null) return isNegated;

        var actual = element.Value.ValueKind switch
        {
            JsonValueKind.String => element.Value.GetString(),
            JsonValueKind.Number => element.Value.GetRawText(),
            JsonValueKind.True => "true",
            JsonValueKind.False => "false",
            _ => null
        };

        var matches = string.Equals(actual, valuePart, StringComparison.OrdinalIgnoreCase);
        return isNegated ? !matches : matches;
    }
}

/// <summary>
/// Definition of a single queryable property within a dynamic schema.
/// </summary>
/// <param name="Name">The property/field name for indexing and querying.</param>
/// <param name="ClrType">The CLR type for Lucene field type mapping.</param>
/// <param name="IsAnalyzed">Whether the field is analyzed (tokenized for full-text search).</param>
/// <param name="JsonPath">Optional JSONPath to extract the value. Defaults to the Name (top-level property).</param>
/// <param name="IsVectorField">Whether vector embeddings are enabled for this property.</param>
public record IndexedJsonProperty(string Name, Type ClrType, bool IsAnalyzed, string? JsonPath = null, bool IsVectorField = false);
