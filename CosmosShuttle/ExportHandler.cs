using Microsoft.Azure.Cosmos;
using System.Diagnostics;
using System.Text.Json;

namespace CosmosShuttle;

public class ExportHandler : IHandler
{
    static readonly byte comma = Convert.ToByte(',');
    static readonly byte newline = Convert.ToByte('\n');

    public async Task Run(Command command)
    {
        (var container, _) = await CosmosUtils.ConnectContainer(command);

        var timestamp = DateTime.Now.ToString("yyyy-MM-ddTHH-mm-ss");
        var fileName = $"cosmos-export-{command.DatabaseName}-{command.ContainerName}-{timestamp}.json";
        var path = Path.Combine(Environment.CurrentDirectory, fileName);
        using var file = File.Create(path);
        file.WriteByte(Convert.ToByte('['));
        file.WriteByte(newline);

        // Pre-count items in container
        using var countResultSet = container.GetItemQueryStreamIterator("SELECT VALUE COUNT(1) FROM c");
        int expectedCount = 0;
        while (countResultSet.HasMoreResults)
        {
            using ResponseMessage response = await countResultSet.ReadNextAsync();
            if (!response.IsSuccessStatusCode)
            {
                Console.WriteLine($"Count operation failed with status: {response.StatusCode}");
                return;
            }

            var json = JsonDocument.Parse(response.Content);
            var documents = json.RootElement.GetProperty("Documents");
            using var items = documents.EnumerateArray();
            expectedCount = items.First().GetInt32();
        }

        Console.WriteLine($"Starting export of {expectedCount} expected items");
        int total = 0;
        var sw = Stopwatch.StartNew();
        using var resultSet = container.GetItemQueryStreamIterator("SELECT * from c");
        while (resultSet.HasMoreResults)
        {
            using ResponseMessage response = await resultSet.ReadNextAsync();
            if (!response.IsSuccessStatusCode)
            {
                Console.WriteLine($"Query failed with status: {response.StatusCode}");
                return;
            }

            response.Headers.TryGetValue("x-ms-item-count", out string countRaw);
            _ = int.TryParse(countRaw, out int count);
            if (count == 0) continue;


            var json = JsonDocument.Parse(response.Content);
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

            Console.SetCursorPosition(0, Console.CursorTop);
            Console.Write($"Processed items: {total} ({(double)total / expectedCount * 100:F2}%)");
        }

        file.WriteByte(newline);
        file.WriteByte(Convert.ToByte(']'));
        file.WriteByte(newline);
        file.Flush();

        Console.WriteLine();
        Console.WriteLine($"Finished exporting {total} items, elapsed: {sw.Elapsed}");
        Console.WriteLine($"Created file: {path}");
    }
}
