namespace CosmosShuttle;

public interface IHandler
{
    ValueTask Run(Command command);
}
