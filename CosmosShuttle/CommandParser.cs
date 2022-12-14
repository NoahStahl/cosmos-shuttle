using System.Diagnostics.CodeAnalysis;

namespace CosmosShuttle;

public static class CommandParser
{
    public const int BatchsizeDefault = 25;
    public const int TimeoutSecondsDefault = 120;

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

        if (args.Length.IsEven())
        {
            throw new Exception("Invalid parameters. Expected matched pairs of ' --option value'. Is a value missing?");
        }

        var options = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        for (int i = 1; i < args.Length - 1; i++)
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

        long? after = null;
        if (options.TryGetValue("after", out string? afterRaw))
        {
            if (!TryParseTimestamp(afterRaw, out after))
            {
                throw new Exception("Invalid value provided for --after. Must be timestamp in either ISO 8601 or UNIX seconds format.");
            }
        }

        long? before = null;
        if (options.TryGetValue("before", out string? beforeRaw))
        {
            if (after.HasValue)
            {
                throw new ArgumentException("Cannot specify both --before and --after");
            }

            if (!TryParseTimestamp(beforeRaw, out before))
            {
                throw new Exception("Invalid value provided for --before. Must be timestamp in either ISO 8601 or UNIX seconds format.");
            }
        }

        TimeRange? timerange = null;
        if (options.TryGetValue("timerange", out string? timerangeRaw))
        {
            if (after.HasValue || before.HasValue)
            {
                throw new ArgumentException("Cannot specify --timerange with either --before or --after");
            }

            if (!TryParseTimeRange(timerangeRaw, out timerange))
            {
                throw new Exception("Invalid value provided for --timerange. Must be pair of timestamps in either ISO 8601 or UNIX seconds format, separated by | (pipe), ex: 1650112000|1670544000");
            }
        }

        int batchSize = BatchsizeDefault;
        if (options.TryGetValue("batchsize", out string? batchsizeRaw)
            && (!int.TryParse(batchsizeRaw, out batchSize) || batchSize < 1 || batchSize > 500))
        {
            throw new Exception("Invalid value provided for --batchsize. Must be positive integer 1 < 500.");
        }

        var logLevel = LogLevel.Info;
        if (options.TryGetValue("logging", out string? loggingRaw) && (!Enum.TryParse(loggingRaw, ignoreCase: true, out logLevel)))
        {
            throw new Exception("Invalid value provided for --logging. Can be 'info' or 'verbose'.");
        }

        var camelCase = false;
        if (options.TryGetValue("camelcase", out string? camelCaseRaw) && !bool.TryParse(camelCaseRaw, out camelCase))
        {
            throw new Exception("Invalid value provided for --camelcase. Can be 'true' or 'false' (default = false).");
        }

        var timeoutSeconds = TimeoutSecondsDefault;
        if (options.TryGetValue("timeout", out string? timeoutRaw)
            && (!int.TryParse(timeoutRaw, out timeoutSeconds) || timeoutSeconds < 1))
        {
            throw new Exception("Invalid value provided for --timeout. Must be positive integer representing seconds to wait for requests to complete. (default = 120)");
        }

        return new()
        {
            After = after,
            Before = before,
            BatchSize = batchSize,
            BaseCommand = baseCommand,
            Camelcase = camelCase,
            ConnectionString = connectionString,
            ContainerName = containerName,
            DatabaseName = databaseName,
            LogLevel = logLevel,
            Source = source,
            TimeRange = timerange,
            TimeoutSeconds = timeoutSeconds
        };
    }

    static bool TryParseTimeRange(string value, [NotNullWhen(true)] out TimeRange? parsed)
    {
        parsed = null;
        var splitIndex = value.IndexOf('|');
        if (splitIndex < 0)
        {
            return false;
        }

        if (!TryParseTimestamp(value.AsSpan(0, splitIndex), out long? start)
            || !TryParseTimestamp(value.AsSpan(splitIndex + 1), out long? end))
        {
            return false;
        }

        parsed = new(start.Value, end.Value);
        return true;
    }

    /// <summary>
    /// Parse string value into UNIX timestamp. Allows either UNIX epoch or ISO string datetime.
    /// </summary>
    static bool TryParseTimestamp(ReadOnlySpan<char> value, [NotNullWhen(true)] out long? parsed)
    {
        parsed = null;
        if (long.TryParse(value, out long parsedUnixSeconds))
        {
            parsed = parsedUnixSeconds;
        }
        else if (DateTime.TryParse(value, out DateTime dateTime))
        {
            parsed = ((DateTimeOffset)dateTime).ToUnixTimeSeconds();
        }

        return parsed.HasValue;
    }
}

public sealed record Command
{
    /// <summary>
    /// Optional timestamp parameter to query items after specified Unix time
    /// </summary>
    public long? After { get; init; }

    /// <summary>
    /// Optional timestamp parameter to query items before specified Unix time
    /// </summary>
    public long? Before { get; init; }

    public CommandType BaseCommand { get; init; }

    public int BatchSize { get; init; } = 25;

    public bool Camelcase { get; init; }

    public string ContainerName { get; init; } = string.Empty;

    public string DatabaseName { get; init; } = string.Empty;

    public string ConnectionString { get; init; } = string.Empty;

    public LogLevel LogLevel { get; init; }

    public TimeRange? TimeRange { get; init; }

    public string? Source { get; init; }

    public int TimeoutSeconds { get; init; } = CommandParser.TimeoutSecondsDefault;
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

/// <summary>
/// Represents a period between two Unix times
/// </summary>
public record TimeRange(long Start, long End);
