using System.Text.Json;

namespace Lotta;

/// <summary>
/// Defines a schema for dynamic (JSON Schema-defined) documents.
/// Only queryable properties are declared — the full JSON document is stored and retrieved as-is.
/// </summary>
public class DynamicSchema
{
    /// <summary>Prefix applied to schema names when stored in the Type column and Lucene _type_ field.
    /// Clearly separates dynamic documents from CLR-typed entities.</summary>
    internal const string StoragePrefix = "_dynamic_";

    /// <summary>Returns the prefixed type name used in storage and Lucene (e.g. "_dynamic_Person").</summary>
    internal string StorageTypeName => StoragePrefix + TypeName;

    /// <summary>Returns true if the given stored type name is a dynamic schema type.</summary>
    internal static bool IsDynamicTypeName(string? typeName) => typeName != null && typeName.StartsWith(StoragePrefix);

    /// <summary>Strips the storage prefix to recover the original schema name.</summary>
    internal static string UnprefixTypeName(string typeName) => typeName.Substring(StoragePrefix.Length);

    /// <summary>A unique name for this document type (e.g. "Person"). Used as the type discriminator in storage and search.</summary>
    public string TypeName { get; set; } = null!;

    /// <summary>The JSON property name used as the document key. Defaults to "Id".</summary>
    public string KeyProperty { get; set; } = "Id";

    /// <summary>Key generation strategy. <see cref="Lotta.KeyMode.Auto"/> generates a ULID when the key is missing; <see cref="Lotta.KeyMode.Manual"/> requires the caller to supply it.</summary>
    public KeyMode KeyMode { get; set; } = KeyMode.Auto;

    /// <summary>The queryable properties defined by this schema. These are indexed in Lucene and promoted to Table Storage columns.</summary>
    public List<SchemaPropertyDef> Properties { get; set; } = new();

    /// <summary>
    /// Parse a JSON Schema subset into a DynamicSchema.
    /// Expected format:
    /// <code>
    /// {
    ///   "properties": { "Name": { "type": "string" }, "Age": { "type": "integer" } },
    ///   "key": "Id",        // optional, default "Id"
    ///   "keyMode": "Auto"   // optional, default "Auto"
    /// }
    /// </code>
    /// </summary>
    public static DynamicSchema Parse(string typeName, JsonElement schema)
    {
        var result = new DynamicSchema { TypeName = typeName };

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
                var isAnalyzed = clrType == typeof(string); // strings analyzed by default, matching QueryableMode.Auto
                result.Properties.Add(new SchemaPropertyDef(prop.Name, clrType, isAnalyzed));
            }
        }

        return result;
    }

    /// <summary>
    /// Extract the key value from a JSON document, or generate a ULID for Auto mode.
    /// </summary>
    public string ExtractKey(JsonElement json)
    {
        if (json.TryGetProperty(KeyProperty, out var keyVal))
        {
            var key = keyVal.ToString();
            if (!string.IsNullOrEmpty(key))
                return key;
        }

        if (KeyMode == KeyMode.Auto)
            return Ulid.NewUlid().ToString();

        throw new InvalidOperationException(
            $"Key property '{KeyProperty}' is missing or empty in JSON document for schema '{TypeName}' with Manual key mode.");
    }

    /// <summary>Extract the key from a JsonDocument.</summary>
    public string ExtractKey(JsonDocument json) => ExtractKey(json.RootElement);

    /// <summary>
    /// Returns a new JsonDocument with the key property set to the given value.
    /// </summary>
    public JsonDocument SetKey(JsonDocument json, string key)
    {
        var dict = new Dictionary<string, JsonElement>();

        foreach (var prop in json.RootElement.EnumerateObject())
            dict[prop.Name] = prop.Value.Clone();

        dict[KeyProperty] = JsonSerializer.SerializeToElement(key);

        var bytes = JsonSerializer.SerializeToUtf8Bytes(dict);
        return JsonDocument.Parse(bytes);
    }

    /// <summary>
    /// Returns a new JsonElement with the key property set to the given value.
    /// </summary>
    public JsonElement SetKey(JsonElement json, string key)
    {
        using var doc = JsonDocument.Parse(json.GetRawText());
        var dict = new Dictionary<string, JsonElement>();

        foreach (var prop in doc.RootElement.EnumerateObject())
            dict[prop.Name] = prop.Value.Clone();

        dict[KeyProperty] = JsonSerializer.SerializeToElement(key);

        return JsonSerializer.SerializeToElement(dict);
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
}

/// <summary>
/// Definition of a single queryable property within a dynamic schema.
/// </summary>
public record SchemaPropertyDef(string Name, Type ClrType, bool IsAnalyzed);
