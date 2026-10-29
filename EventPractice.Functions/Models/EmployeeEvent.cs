namespace EventPractice.Functions.Models;

public class EmployeeEvent
{
    public Guid Id { get; set; }
    public string EventType { get; set; } = "";
    public string Name { get; set; } = "";
    public string Department { get; set; } = "";
}