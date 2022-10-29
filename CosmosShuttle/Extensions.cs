using System.Text.Json;

namespace CosmosShuttle;

public static class Extensions
{
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
