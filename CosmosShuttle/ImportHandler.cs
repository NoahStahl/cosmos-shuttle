using Microsoft.Azure.Cosmos;
using System.Diagnostics;
using System.Text.Json;

namespace CosmosShuttle;

public class ImportHandler : IHandler
{
    static readonly ItemRequestOptions operationOptions = new() { EnableContentResponseOnWrite = false };

    public async Task Run(Command command)
    {
        if (string.IsNullOrWhiteSpace(command.Source))
        {
            throw new ArgumentException("Missing option --source");
        }

        if (!File.Exists(command.Source))
        {
            throw new ArgumentException($"File not found: {command.Source}");
        }

        using var file = File.OpenRead(command.Source);
        using var json = await JsonDocument.ParseAsync(file, new() { AllowTrailingCommas = true });
        using JsonElement.ArrayEnumerator items = json.RootElement.EnumerateArray();

        (var container, string? pkProperty) = await CosmosUtils.ConnectContainer(command);

        Console.WriteLine($"Starting import of items from {command.Source}");
        Console.WriteLine($"Batch size: {command.BatchSize}");
        int succeeded = 0;
        int failed = 0;
        int itemCount = 0;
        int processedCount = 0;
        var sw = Stopwatch.StartNew();
        var batch = new List<BatchedOperation>(command.BatchSize);
        int batchIndex = 1;
        foreach (var item in items)
        {
            itemCount++;
            string? partition = null;
            if (pkProperty is not null && item.TryGetProperty(pkProperty, out var pk))
            {
                partition = pk.GetString();
            }
            using MemoryStream stream = new();
            using Utf8JsonWriter writer = new(stream);
            item.WriteTo(writer);
            writer.Flush();
            var task = container.UpsertItemStreamAsync(stream, new(partition), operationOptions);
            string? itemId = item.TryGetProperty("id", out var id) ? id.GetString() : null;
            batch.Add(new(itemId, task));

            if (batch.Count >= command.BatchSize)
            {
                await ProcessBatch();
                batch.Clear();
                batchIndex++;
            }
        }

        if (batch.Count > 0)
        {
            await ProcessBatch();
        }

        file.Close();

        Console.WriteLine();
        Console.WriteLine($"Finished importing {itemCount} items, elapsed: {sw.Elapsed}");
        Console.WriteLine($"Succeeded: {succeeded}");
        Console.WriteLine($"Failed: {failed}");

        async Task ProcessBatch()
        {
            int start = (batchIndex - 1) * command.BatchSize;
            int end = Math.Min(batchIndex * command.BatchSize, itemCount);
            processedCount += end - start;
            Console.SetCursorPosition(0, Console.CursorTop);
            Console.Write($"Processed items: {processedCount} ({(double)processedCount / itemCount * 100:F2}%)");
            var batchTasks = batch.Select(i => i.Task).ToArray();
            await Task.WhenAll(batchTasks);
            foreach (var completedTask in batchTasks)
            {
                var response = await completedTask;
                if (response.IsSuccessStatusCode)
                {
                    succeeded++;
                }
                else
                {
                    failed++;
                    var failedItemId = batch.FirstOrDefault(i => i.Task.Id == completedTask.Id)?.ItemId;
                    Console.WriteLine($"Failed to import item with id '{failedItemId}', response status: {response.StatusCode}");
                }
            }
        }
    }
}

public record BatchedOperation(string? ItemId, Task<ResponseMessage> Task);
