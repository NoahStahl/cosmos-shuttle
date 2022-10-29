using CosmosShuttle;

Console.WriteLine("Welcome to Cosmos Shuttle!");

try
{
    var command = CommandParser.Parse(args);
    IHandler handler = command.BaseCommand switch
    {
        CommandType.Export => new ExportHandler(),
        CommandType.Import => new ImportHandler(),
        _ => throw new InvalidOperationException("Operation not recognized")
    };
    await handler.Run(command);

#if DEBUG
    Console.ReadKey();
#endif
}
catch (Exception ex)
{
    Console.WriteLine($"Something went wrong: {ex.Message}");
}
