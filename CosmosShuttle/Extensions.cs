using System.Text.Json;
using System.Text.Json.Nodes;

namespace CosmosShuttle;

public static class Extensions
{
    private static readonly Dictionary<string, string> camelCaseCache = new();

    public static IReadOnlyList<KeyTransform> GetCamelcaseTransforms(this JsonObject? node)
    {
        if (node is null)
        {
            return Array.Empty<KeyTransform>();
        }

        List<KeyTransform>? transforms = null;
        foreach (var property in node)
        {
            if (property.Key.TryCamelCase(out string camelKey))
            {
                (transforms ??= new()).Add(new(property.Key, camelKey));
            }
        }

        if (transforms is null)
        {
            return Array.Empty<KeyTransform>();
        }

        return transforms;
    }

    public static bool IsEven(this int value) => value % 2 == 0;

    public static bool TryCamelCase(this string input, out string result)
    {
        result = input;
        if (input.Length == 0 || !char.IsUpper(input[0]))
        {
            return false;
        }

        if (camelCaseCache.TryGetValue(input, out string? cachedValue))
        {
            result = cachedValue;
            return true;
        }

        result = $"{char.ToLower(input[0])}{input[1..]}";
        camelCaseCache.TryAdd(input, result);

        return true;
    }

    /// <summary>
    /// Get first property with specified key matched ignoring case
    /// </summary>
    public static (bool found, string? key) TryGetPropertyIgnoreCase(this JsonElement element, string name, out JsonElement value)
    {
        value = default;
        if (element.ValueKind != JsonValueKind.Object)
        {
            return (false, null);
        }

        // Prefer fast case-sensitive lookup
        if (element.TryGetProperty(name, out var exactMatchedValue))
        {
            value = exactMatchedValue;
            return (true, name);
        }

        // Fall back to case insensitive property name comparison
        foreach (var property in element.EnumerateObject())
        {
            if (property.Name.Equals(name, StringComparison.OrdinalIgnoreCase))
            {
                value = property.Value;
                return (true, property.Name);
            }
        }

        return (false, null);
    }
}

public readonly record struct KeyTransform(string From, string To);
