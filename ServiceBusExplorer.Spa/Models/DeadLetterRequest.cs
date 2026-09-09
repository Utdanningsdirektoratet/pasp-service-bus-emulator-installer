namespace ServiceBusExplorer.Models;

public record DeadLetterRequest(string? Reason, string? Description);
