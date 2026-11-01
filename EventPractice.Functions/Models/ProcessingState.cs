namespace EventPractice.Functions.Models;

public class ProcessingState
{
    public string Id { get; set; } = "";        // event ki id
    public string Status { get; set; } = "";    // InProgress / Completed / Failed
    public string OwnerId { get; set; } = "";   // kis worker ne claim kiya
    public string? Error { get; set; }
    public DateTime UpdatedAtUtc { get; set; }
}