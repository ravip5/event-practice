using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var builder = FunctionsApplication.CreateBuilder(args);

// Logging / monitoring
builder.Services
    .AddApplicationInsightsTelemetryWorkerService()
    .ConfigureFunctionsApplicationInsights();

// CosmosClient: poori app mein ek hi (Singleton)
builder.Services.AddSingleton(sp =>
{
    var config = sp.GetRequiredService<IConfiguration>();
    return new CosmosClient(config["CosmosConnection"], new CosmosClientOptions
    {
        SerializerOptions = new CosmosSerializationOptions
        {
            PropertyNamingPolicy = CosmosPropertyNamingPolicy.CamelCase
        }
    });
});

var app = builder.Build();

// Database aur container nahi hain toh bana do
var cosmos = app.Services.GetRequiredService<CosmosClient>();
var db = await cosmos.CreateDatabaseIfNotExistsAsync("EventStore");
await db.Database.CreateContainerIfNotExistsAsync("events", "/id");
await db.Database.CreateContainerIfNotExistsAsync("processing-state", "/id");
app.Run();