using Microsoft.Azure.Cosmos;

namespace CosmosShuttle;

public static class CosmosUtils
{
    public static async Task<(Container, string?)> ConnectContainer(Command command)
    {
        Console.WriteLine($"Connecting to database {command.DatabaseName}, container {command.ContainerName}");

        var client = new CosmosClient(command.ConnectionString, new() { AllowBulkExecution = true });
        var database = client.GetDatabase(command.DatabaseName);
        try
        {
            var databaseProperties = await database.ReadAsync();
            Console.WriteLine($"Successfully connected to database {databaseProperties.Database.Id}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to connect to database: {ex.Message}");
            throw;
        }
        var container = client.GetContainer(command.DatabaseName, command.ContainerName);
        var props = await container.ReadContainerAsync();
        var paritionKeyProperty = props.Resource.PartitionKeyPath?.TrimStart('/');

        return (container, paritionKeyProperty);
    }
}
