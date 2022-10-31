using System.Text.Json;
using System.Text.Json.Nodes;

namespace CosmosShuttle;

public static class Extensions
{
    private static readonly Dictionary<string, string> camelCaseCache = new();

    /// <summary>
    /// Returns item as JsonOject with camelcased keys
    /// </summary>
    /// <param name="keys">Optional list of specific keys to transform</param>
    /// <returns>JsonObject with modified keys if changes, or null if no changes required</returns>
    /// <exception cref="InvalidDataException">When item cannot be parsed as a JSON object</exception>
    public static JsonObject? CamelCaseKeys(this JsonElement item, IReadOnlyList<string>? keys = null)
    {
        var transforms = item.GetCamelcaseTransforms();
        if (transforms.Count == 0)
        {
            return null;
        }

        // Transform all keys to camelCase. Remove/add each key to maintain original order, even if already correct.
        var node = JsonSerializer.Deserialize<JsonNode>(item)?.AsObject() ?? throw new InvalidDataException($"Failed to parse data item: {item}");
        foreach (var property in node.ToArray())
        {
            var value = node[property.Key];   // Stash value
            node.Remove(property.Key);        // Remove old key

            // Add new key
            if (transforms.TryGetValue(property.Key, out string? camelKey)
                && (keys is null || keys.Any(i => i.Equals(property.Key, StringComparison.OrdinalIgnoreCase))))
            {
                node.TryAdd(camelKey, value);
            }
            else
            {
                node.TryAdd(property.Key, value);
            }
        }

        return node;
    }

    public static void ClearConsoleLine()
    {
        Console.SetCursorPosition(0, Console.CursorTop);
        for (int i = 0; i < Console.WindowWidth; i++) Console.Write(" ");
        Console.SetCursorPosition(0, Console.CursorTop);
    }

    public static IReadOnlyDictionary<string, string> GetCamelcaseTransforms(this JsonElement item)
    {
        if (item.ValueKind != JsonValueKind.Object)
        {
            throw new InvalidOperationException($"Expected object value, but received {item.ValueKind}");
        }

        var transforms = new Dictionary<string, string>();
        foreach (var property in item.EnumerateObject())
        {
            if (property.Name.TryCamelCase(out string camelKey))
            {
                transforms.TryAdd(property.Name, camelKey);
            }
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
