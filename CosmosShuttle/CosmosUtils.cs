using Microsoft.Azure.Cosmos;

namespace CosmosShuttle;

public static class CosmosUtils
{
    const string LocalEmulatorConnection = "AccountEndpoint=https://localhost:8081/;AccountKey=C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";

    public static async ValueTask<ConnectionResult> ConnectContainer(Command command)
    {
        var connectionString = command.ConnectionString.Equals("emulator", StringComparison.OrdinalIgnoreCase)
            ? LocalEmulatorConnection
            : command.ConnectionString;

        var client = new CosmosClient(connectionString, new()
        {
            AllowBulkExecution = true,
            RequestTimeout = TimeSpan.FromSeconds(command.TimeoutSeconds)
        });

        Console.WriteLine($"Connecting to {client.Endpoint.Host} > {command.DatabaseName} > {command.ContainerName}");
        var database = client.GetDatabase(command.DatabaseName);
        DatabaseResponse? databaseProperties = null;
        try
        {
            databaseProperties = await database.ReadAsync();
            Console.WriteLine($"Successfully connected to database {databaseProperties.Database.Id}");

            var container = client.GetContainer(command.DatabaseName, command.ContainerName);
            var props = await container.ReadContainerAsync();
            var partitionKeyProperty = props.Resource.PartitionKeyPath?.TrimStart('/');

            return new(container, partitionKeyProperty);
        }
        catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            var message = databaseProperties is null
                ? $"No database found with name: {command.DatabaseName}"
                : $"No container found with name: {command.ContainerName}";
            Console.WriteLine(message);

            if (command.LogLevel == LogLevel.Verbose)
            {
                Console.WriteLine(ex.Message);
            }

            return new(null, null);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to connect: {ex.Message}");

            if (command.LogLevel == LogLevel.Verbose)
            {
                Console.WriteLine(ex.Message);
            }

            return new(null, null);
        }
    }
}

public sealed record ConnectionResult(Container? Container, string? PartitionKeyProperty);
