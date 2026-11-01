using System.Net;
using System.Text.Json;
using Azure.Messaging.EventHubs;
using EventPractice.Functions.Models;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace EventPractice.Functions;

public class EmployeeEventListener
{
    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };
    private readonly ILogger<EmployeeEventListener> _logger;
    private readonly Container _container;

    public EmployeeEventListener(ILogger<EmployeeEventListener> logger, CosmosClient cosmos, IConfiguration config)
    {
        _logger = logger;
        _container = cosmos.GetContainer(config["CosmosDatabase"], config["CosmosContainer"]);
    }

    [Function(nameof(EmployeeEventListener))]
    public async Task Run(
        [EventHubTrigger("employee-events", Connection = "EventHubConnection", ConsumerGroup = "practice-cg")]
        EventData[] events,
        CancellationToken ct)
    {
        foreach (var e in events)
        {
            EmployeeEvent? evt = null;
            try
            {
                evt = JsonSerializer.Deserialize<EmployeeEvent>(e.EventBody.ToString(), JsonOptions);

                if (evt is null || evt.Id == Guid.Empty)
                {
                    _logger.LogWarning("Seq {Seq}: Id missing hai, skip", e.SequenceNumber);
                    continue;
                }

                if (evt.EventType != "EmployeeCreated")
                {
                    _logger.LogInformation("Seq {Seq}: {Type} hamare kaam ka nahi, skip", e.SequenceNumber, evt.EventType);
                    continue;
                }

                await _container.CreateItemAsync(evt, new PartitionKey(evt.Id.ToString()), cancellationToken: ct);
                _logger.LogInformation("Seq {Seq}: ✅ {Name} ({Id}) Cosmos mein save", e.SequenceNumber, evt.Name, evt.Id);
            }
            catch (JsonException ex)
            {
                _logger.LogError(ex, "Seq {Seq}: ❌ Kharab JSON, skip", e.SequenceNumber);
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.Conflict)
            {
                _logger.LogWarning("Seq {Seq}: 🔁 Id {Id} pehle se saved hai, duplicate skip", e.SequenceNumber, evt?.Id);
            }
        }
    }
}