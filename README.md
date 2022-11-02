# Cosmos Shuttle

Cosmos Shuttle is a simple data import/export tool for Cosmos DB.

# Building the project

To build the project into the tool executable:

1. Clone this repository
2. From a command line terminal, change to `.../cosmos-shuttle/CosmosShuttle`
3. Run build command: `dotnet publish -c Release -r win-x64 --self-contained`
4. Locate the produced `CosmosShuttle.exe` file under `.../bin/Release/net6.0/win-x64/publish`


# Running the tool

Given a built `CosmosShuttle.exe` executable, you can run it to perform an import or export.

## Obtaining a connection string

The tool authenticates with a Cosmos account connection string. This can be found in Azure Portal > Cosmos account > Settings > Keys. 

When exporting, a read-only key will suffice. When importing, a read-write key is required.

## Export data from Cosmos DB

`CosmosShuttle.exe export --db <DATABASE_NAME> --container <CONTAINER_NAME> --connection "<CONNECTION_STRING>"`

where:

- DATABASE_NAME: Source Cosmos DB database
- CONTAINER_NAME: identifier of a source container from which to export all items
- CONNECTION_STRING: connection string value (Read-only or Read-Write)

If successful, a JSON file is created representing all the items in the container.

## Import data into Cosmos DB

`CosmosShuttle.exe import --source <SOURCE_FILE> --db <DATABASE_NAME> --container <CONTAINER_NAME> --batchsize <BATCH_SIZE> --connection "<CONNECTION_STRING>"`

Emulator example:

`CosmosShuttle.exe import --source export.json --db contoso-db --container data --connection emulator`

Available parameters:

- `batchsize`: Number of upsert operations to perform in parallel batches. `1` to `500` allowed. Default: `25`
- `camelcase`: Specifying `true` converts all items to have camelCased property keys during import
- `connecton`: connection string value (Read-Write). May use `emulator` as shortcut for local emulator conneciton string. 
- `container`: identifier of a target container into which to import all items
- `db`: Target Cosmos DB database
- `logging`: Controls verbosity of output. Levels: `info` | `verbose`
- `source`: Path to the JSON file exported by this tool. This data will be imported into the target container.
- `timeout`: Number of seconds to wait for responses from Cosmos DB. Default: `120`

If successful, all items in the source file will be created or updated in the target container.

The tool uses an upsert operation based on the item's `id` value. If an item already exists in the target container with the same `id`, it will be overwritten with the source item. Otherwise, it will be created.
