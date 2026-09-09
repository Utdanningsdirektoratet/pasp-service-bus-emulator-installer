namespace ServiceBusExplorer.Models;

public record SendMessageRequest(string Body, string? Subject, string? ContentType);
