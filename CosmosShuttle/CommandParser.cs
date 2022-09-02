namespace CosmosShuttle;

public static class CommandParser
{
    public static Command Parse(string[] args)
    {
        if (args.Length < 3)
        {
            throw new Exception("Invalid command. Syntax: CosmosShuttle <operation> [...parameters]");
        }

        if (!Enum.TryParse<CommandType>(args[0], true, out var baseCommand))
        {
            throw new Exception("Invalid operation. Must be 'export' or 'import'");
        }

        var options = new Dictionary<string, string>();
        for (int i = 1; i < args.Length; i++)
        {
            if (args[i].StartsWith("--"))
            {
                options.Add(args[i][2..], args[i + 1]);
                i++;
            }
        }

        if (!options.TryGetValue("container", out string? containerName))
        {
            throw new Exception("Missing option --container");
        }
        if (!options.TryGetValue("db", out string? databaseName))
        {
            throw new Exception("Missing option --db");
        }
        if (!options.TryGetValue("connection", out string? connectionString))
        {
            throw new Exception("Missing option --connection");
        }
        if (!options.TryGetValue("source", out string? source) && baseCommand == CommandType.Import)
        {
            throw new Exception("Missing option --source");
        }

        int batchSize = 25;
        if (options.TryGetValue("batchsize", out string? batchsizeRaw) 
            && (!int.TryParse(batchsizeRaw, out batchSize) || batchSize < 1 || batchSize > 500))
        {
            throw new Exception("Invalid value provided for --batchsize. Must be positive integer 1 < 500");
        }

        return new()
        {
            BatchSize = batchSize,
            BaseCommand = baseCommand,
            ConnectionString = connectionString,
            ContainerName = containerName,
            DatabaseName = databaseName,
            Source = source
        };
    }
}

public record Command
{
    public CommandType BaseCommand { get; set; }
    public string DatabaseName { get; set; } = string.Empty;
    public string ContainerName { get; set; } = string.Empty;
    public string ConnectionString { get; set; } = string.Empty;
    public string? Source { get; set; }
    public int BatchSize { get; set; } = 25;
}

public enum CommandType
{
    None,
    Export,
    Import
}
