﻿using Microsoft.Azure.Cosmos;
using System.Diagnostics;
using System.Text.Json;

namespace CosmosShuttle;

public sealed class ExportHandler : IHandler
{
    static readonly byte comma = Convert.ToByte(',');
    static readonly byte newline = Convert.ToByte('\n');

    public async ValueTask Run(Command command)
    {
        (var container, _) = await CosmosUtils.ConnectContainer(command);
        if (container is null)
        {
            Console.WriteLine("Stopping due to failed connection");
            return;
        }

        // Map optional filter clause from command
        string? filter = null;
        if (command.After.HasValue)
        {
            filter = $"c._ts >= {command.After.Value}";
        }
        else if (command.Before.HasValue)
        {
            filter = $"c._ts <= {command.Before.Value}";
        }
        else if (command.TimeRange is not null)
        {
            filter = $"c._ts >= {command.TimeRange.Start} AND c._ts <= {command.TimeRange.End}";
        }

        // Pre-count items in container
        int expectedCount = await CountItems(container, filter);
        Console.WriteLine($"Starting export of {expectedCount} expected items");

        // Create file
        var timestamp = DateTime.Now.ToString("yyyy-MM-ddTHH-mm-ss");
        var fileName = $"cosmos-export-{command.DatabaseName}-{command.ContainerName}-{timestamp}.json";
        var path = Path.Combine(Environment.CurrentDirectory, fileName);
        using var file = File.Create(path);
        file.WriteByte(Convert.ToByte('['));
        file.WriteByte(newline);

        int total = 0;
        var sw = Stopwatch.StartNew();
        const string baseQuery = "SELECT * FROM c";
        var query = filter is not null ? $"{baseQuery} WHERE {filter}" : baseQuery;
        Console.WriteLine($"Using query: {query}");
        using var resultSet = container.GetItemQueryStreamIterator(query);
        while (resultSet.HasMoreResults)
        {
            using ResponseMessage response = await resultSet.ReadNextAsync();
            if (!response.IsSuccessStatusCode)
            {
                Console.WriteLine($"Query failed with status: {response.StatusCode} and message {response.ErrorMessage}");
                return;
            }

            response.Headers.TryGetValue("x-ms-item-count", out string? countRaw);
            _ = int.TryParse(countRaw, out int count);
            if (count == 0) continue;


            using var json = JsonDocument.Parse(response.Content);
            var documents = json.RootElement.GetProperty("Documents");

            int i = 0;
            using var items = documents.EnumerateArray();
            foreach (var item in items)
            {
                JsonSerializer.Serialize(file, item);
                total++;
                if (++i < count || resultSet.HasMoreResults)
                {
                    file.WriteByte(comma);
                    file.WriteByte(newline);
                }
            }

            file.Flush();

            var rate = Math.Min(total, Math.Round(total / sw.Elapsed.TotalSeconds));
            Extensions.ClearConsoleLine();
            Console.Write($"Processed items: {total} | {(double)total / expectedCount * 100:F2}% | {rate}/sec");
        }

        file.WriteByte(newline);
        file.WriteByte(Convert.ToByte(']'));
        file.WriteByte(newline);
        file.Flush();

        Console.WriteLine();
        Console.WriteLine($"Finished exporting {total} items, elapsed: {sw.Elapsed.FormatDisplay()}");
        Console.WriteLine($"Created file: {path}");
    }

    static async ValueTask<int> CountItems(Container container, string? filter)
    {
        const string baseQuery = "SELECT VALUE COUNT(1) FROM c";
        var query = filter is not null ? $"{baseQuery} WHERE {filter}" : baseQuery;
        using var countResultSet = container.GetItemQueryStreamIterator(query);
        int expectedCount = 0;
        while (countResultSet.HasMoreResults)
        {
            using ResponseMessage response = await countResultSet.ReadNextAsync();
            if (!response.IsSuccessStatusCode)
            {
                Console.WriteLine($"Count operation failed with status: {response.StatusCode}");
                return 0;
            }

            using var json = JsonDocument.Parse(response.Content);
            var documents = json.RootElement.GetProperty("Documents");
            using var items = documents.EnumerateArray();
            expectedCount = items.First().GetInt32();
            break;
        }

        return expectedCount;
    }
}
