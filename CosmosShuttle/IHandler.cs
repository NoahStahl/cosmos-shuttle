namespace CosmosShuttle;

public interface IHandler
{
    Task Run(Command command);
}
