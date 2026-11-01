using System.Net;
using EventPractice.Functions.Models;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace EventPractice.Functions;

public class EmployeeChangeFeedProcessor
{
    // Har function instance ki apni pehchaan
    private static readonly string OwnerId = Guid.NewGuid().ToString("N")[..8];

    private readonly ILogger<EmployeeChangeFeedProcessor> _logger;
    private readonly Container _state;

    public EmployeeChangeFeedProcessor(ILogger<EmployeeChangeFeedProcessor> logger, CosmosClient cosmos, IConfiguration config)
    {
        _logger = logger;
        _state = cosmos.GetContainer(config["CosmosDatabase"], config["StateContainer"]);
    }

    [Function(nameof(EmployeeChangeFeedProcessor))]
    public async Task Run(
        [CosmosDBTrigger(
            databaseName: "EventStore",
            containerName: "events",
            Connection = "CosmosConnection",
            LeaseContainerName = "leases",
            CreateLeaseContainerIfNotExists = true,
            StartFromBeginning = true)]
        IReadOnlyList<EmployeeEvent> documents,
        CancellationToken ct)
    {
        foreach (var doc in documents)
        {
            var eventId = doc.Id.ToString();

            // 1️⃣ CLAIM: register mein naam likho
            string etag;
            try
            {
                var claim = new ProcessingState
                {
                    Id = eventId,
                    Status = "InProgress",
                    OwnerId = OwnerId,
                    UpdatedAtUtc = DateTime.UtcNow
                };
                var created = await _state.CreateItemAsync(claim, new PartitionKey(eventId), cancellationToken: ct);
                etag = created.ETag;
                _logger.LogInformation("🔒 {Name}: claim kiya (InProgress), owner {Owner}", doc.Name, OwnerId);
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.Conflict)
            {
                var existing = await _state.ReadItemAsync<ProcessingState>(eventId, new PartitionKey(eventId), cancellationToken: ct);
                _logger.LogWarning("⏭️ {Name}: pehle se {Status} hai, skip", doc.Name, existing.Resource.Status);
                continue;
            }

            // 2️⃣ BUSINESS KAAM
            var result = new ProcessingState { Id = eventId, OwnerId = OwnerId };
            try
            {
                await DoBusinessWorkAsync(doc, ct);
                result.Status = "Completed";
                _logger.LogInformation("✅ {Name}: Completed", doc.Name);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                result.Status = "Failed";
                result.Error = ex.Message;
                _logger.LogError("❌ {Name}: Failed - {Error}", doc.Name, ex.Message);
            }

            // 3️⃣ RESULT SAVE: sirf tab jab sticker (ETag) wahi ho
            result.UpdatedAtUtc = DateTime.UtcNow;
            try
            {
                await _state.ReplaceItemAsync(result, eventId, new PartitionKey(eventId),
                    new ItemRequestOptions { IfMatchEtag = etag }, ct);
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.PreconditionFailed)
            {
                _logger.LogError("🏷️ {Name}: ETag mismatch! Kisi aur ne record badal diya, overwrite nahi kiya", doc.Name);
            }
        }
    }

    // CRM/CAM call ka natak
    private static async Task DoBusinessWorkAsync(EmployeeEvent doc, CancellationToken ct)
    {
        await Task.Delay(500, ct);
        if (doc.Name.Contains("Fail", StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException("CAM ne isSuccess = false bheja");
    }
}