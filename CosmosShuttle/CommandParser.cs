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

        var options = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        for (int i = 1; i < args.Length; i++)
        {
            if (args[i].StartsWith("--"))
            {
                var next = args[i + 1];
                if (next.StartsWith("--"))
                {
                    throw new Exception($"Invalid value for {args[i]}. All parameters use switch followed by value: --option value");
                }

                options.Add(args[i][2..], args[i + 1]);
                i++;
            }
        }

        // Required parameters
        if (!options.TryGetValue("connection", out string? connectionString))
        {
            throw new Exception("Missing option --connection");
        }
        if (!options.TryGetValue("container", out string? containerName))
        {
            throw new Exception("Missing option --container");
        }
        if (!options.TryGetValue("db", out string? databaseName))
        {
            throw new Exception("Missing option --db");
        }
        if (!options.TryGetValue("source", out string? source) && baseCommand == CommandType.Import)
        {
            throw new Exception("Missing option --source");
        }

        // Optional parameters
        int batchSize = 25;
        if (options.TryGetValue("batchsize", out string? batchsizeRaw)
            && (!int.TryParse(batchsizeRaw, out batchSize) || batchSize < 1 || batchSize > 500))
        {
            throw new Exception("Invalid value provided for --batchsize. Must be positive integer 1 < 500");
        }

        var logLevel = LogLevel.Info;
        if (options.TryGetValue("logging", out string? loggingRaw) && (!Enum.TryParse(loggingRaw, ignoreCase: true, out logLevel)))
        {
            throw new Exception("Invalid value provided for --logging. Can be 'info' or 'verbose'");
        }

        var camelCase = false;
        if (options.TryGetValue("camelcase", out string? camelCaseRow) && (!bool.TryParse(camelCaseRow, out camelCase)))
        {
            throw new Exception("Invalid value provided for --camelcase. Can be 'true' or 'false' (default is false)");
        }

        return new()
        {
            BatchSize = batchSize,
            BaseCommand = baseCommand,
            Camelcase= camelCase,
            ConnectionString = connectionString,
            ContainerName = containerName,
            DatabaseName = databaseName,
            LogLevel = logLevel,
            Source = source
        };
    }
}

public record Command
{
    public CommandType BaseCommand { get; set; }

    public int BatchSize { get; set; } = 25;

    public bool Camelcase { get; set; }

    public string ContainerName { get; set; } = string.Empty;

    public string DatabaseName { get; set; } = string.Empty;

    public string ConnectionString { get; set; } = string.Empty;

    public LogLevel LogLevel { get; set; }

    public string? Source { get; set; }
}

public enum CommandType
{
    None,
    Export,
    Import
}

public enum LogLevel
{
    Info,
    Verbose
}
