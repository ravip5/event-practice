using System.Text.Json;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;

const string connectionString =
    "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;";
const string hubName = "employee-events";

await using var producer = new EventHubProducerClient(connectionString, hubName);
using EventDataBatch batch = await producer.CreateBatchAsync();

var events = new List<string>
{
    JsonSerializer.Serialize(new { Id = Guid.NewGuid(), EventType = "EmployeeCreated", Name = "Ravi", Department = "IT" }),
    JsonSerializer.Serialize(new { Id = Guid.NewGuid(), EventType = "EmployeeDeleted", Name = "Amit", Department = "HR" }),
    "{ yeh kharab json hai",
    JsonSerializer.Serialize(new { EventType = "EmployeeCreated", Name = "Bina Id", Department = "IT" }),
    JsonSerializer.Serialize(new { Id = Guid.NewGuid(), EventType = "EmployeeCreated", Name = "Priya", Department = "Finance" })
};

foreach (var json in events)
{
    if (!batch.TryAdd(new EventData(BinaryData.FromString(json))))
        throw new Exception("Event batch mein fit nahi hua");
}
await producer.SendAsync(batch);
Console.WriteLine("5 events bhej diye!");
Console.WriteLine("Band karne ke liye Enter dabao...");
Console.ReadLine();