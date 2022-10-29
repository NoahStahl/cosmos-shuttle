using Microsoft.Azure.Cosmos;
using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace CosmosShuttle;

public class ImportHandler : IHandler
{
    static readonly ItemRequestOptions omitResponseContent = new() { EnableContentResponseOnWrite = false };

    public async ValueTask Run(Command command)
    {
        if (string.IsNullOrWhiteSpace(command.Source))
        {
            throw new ArgumentException("Missing option --source");
        }

        if (!File.Exists(command.Source))
        {
            throw new ArgumentException($"File not found: {command.Source}");
        }

        (var container, string? pkProperty) = await CosmosUtils.ConnectContainer(command);
        if (container is null)
        {
            Console.WriteLine("Stopping due to failed connection");
            return;
        }

        Console.WriteLine($"Starting import of items from {command.Source}");
        Console.WriteLine($"Batch size: {command.BatchSize}");

        using var file = File.OpenRead(command.Source);
        using var json = await JsonDocument.ParseAsync(file, new() { AllowTrailingCommas = true });
        var items = json.RootElement.EnumerateArray();

        int succeeded = 0;
        int failed = 0;
        int index = 0;
        int processedCount = 0;
        var sw = Stopwatch.StartNew();
        var batch = new List<BatchedOperation>(command.BatchSize);
        int batchIndex = 1;
        foreach (var item in items)
        {
            // Ensure id property defined (case-insensitive)
            string? itemId = null;
            (bool idFound, string? key) = item.TryGetPropertyIgnoreCase("id", out var idProp);
            if (!idFound || key is null)
            {
                Console.WriteLine($"! Invalid data: No id value present for item [{index}]");
                return;
            }

            using MemoryStream stream = new();
            using Utf8JsonWriter writer = new(stream);
            if (key == "id")
            {
                itemId = idProp.GetString();
                item.WriteTo(writer);
            }
            else if (!command.Camelcase)
            {
                // Transform ID property to lowercased key
                var node = JsonSerializer.Deserialize<JsonNode>(item)?.AsObject() ?? throw new InvalidOperationException($"Failed to parse data item: {item.ToString()}");
                node.Remove(key);
                node.TryAdd("id", idProp.GetString());
                node.WriteTo(writer);
            }
            else if (command.Camelcase)
            {
                // Transform all keys to camelCase
                var node = JsonSerializer.Deserialize<JsonNode>(item)?.AsObject() ?? throw new InvalidOperationException($"Failed to parse data item: {item.ToString()}");
                var transforms = node.GetCamelcaseTransforms();
                foreach (var transform in transforms)
                {
                    var value = node[transform.From];   // Stash value
                    node.Remove(transform.From);        // Remove old key
                    node.TryAdd(transform.To, value);   // Add new key
                }
                node.WriteTo(writer);
            }
            writer.Flush();

            // Ensure partition key is present if container is partitioned
            string? partition = null;
            if (pkProperty is not null && (!item.TryGetProperty(pkProperty, out var pk) || (partition = pk.GetString()) is null))
            {
                Console.WriteLine($"! Invalid data: Item {itemId ?? $"[{index}]"} is missing expected partition key property '{pkProperty}'");
                return;
            }

            // Build and process operation batches
            var task = container.UpsertItemStreamAsync(stream, new(partition), omitResponseContent);
            batch.Add(new(itemId, task));

            if (batch.Count >= command.BatchSize)
            {
                await ProcessBatch();
                batch.Clear();
                batchIndex++;
            }

            index++;
        }

        if (batch.Count > 0)
        {
            await ProcessBatch();
        }

        file.Close();

        Console.WriteLine();
        Console.WriteLine($"Finished importing {index} items, elapsed: {sw.Elapsed}");
        Console.WriteLine($"Succeeded: {succeeded}");
        Console.WriteLine($"Failed: {failed}");

        async Task ProcessBatch()
        {
            int start = (batchIndex - 1) * command.BatchSize;
            int end = Math.Min(batchIndex * command.BatchSize, index);
            processedCount += end - start;
            Console.SetCursorPosition(0, Console.CursorTop);
            Console.Write($"Processed items: {processedCount} ({(double)processedCount / index * 100:F2}%){Environment.NewLine}");

            var tasks = batch.Select(i => i.Task).ToArray();
            await Task.WhenAll(tasks);
            foreach (var task in tasks)
            {
                var response = await task;
                if (response.IsSuccessStatusCode)
                {
                    succeeded++;
                }
                else
                {
                    failed++;
                    var failedItemId = batch.FirstOrDefault(i => i.Task.Id == task.Id)?.ItemId;
                    Console.WriteLine($"! Failed to import item '{failedItemId}', response status: {response.StatusCode}");
                }
            }
        }
    }
}

public sealed record BatchedOperation(string? ItemId, Task<ResponseMessage> Task);
