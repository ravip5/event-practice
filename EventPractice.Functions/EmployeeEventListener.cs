using System.Text.Json;
using Azure.Messaging.EventHubs;
using EventPractice.Functions.Models;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace EventPractice.Functions;

public class EmployeeEventListener
{
    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };
    private readonly ILogger<EmployeeEventListener> _logger;

    public EmployeeEventListener(ILogger<EmployeeEventListener> logger) => _logger = logger;

    [Function(nameof(EmployeeEventListener))]
    public void Run(
        [EventHubTrigger("employee-events", Connection = "EventHubConnection", ConsumerGroup = "practice-cg")]
        EventData[] events)
    {
        foreach (var e in events)
        {
            try
            {
                var evt = JsonSerializer.Deserialize<EmployeeEvent>(e.EventBody.ToString(), JsonOptions);

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

                _logger.LogInformation("Seq {Seq}: ✅ {Name} ({Id}) process ho gaya", e.SequenceNumber, evt.Name, evt.Id);
            }
            catch (JsonException ex)
            {
                _logger.LogError(ex, "Seq {Seq}: ❌ Kharab JSON, skip. Body: {Body}", e.SequenceNumber, e.EventBody.ToString());
            }
        }
    }
}