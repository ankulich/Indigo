namespace Aggregator.Api.Models;

public class ExchangeServerSettings
{
    public string Name { get; set; } = string.Empty;
    public int Port { get; set; }
    public string WebSocketPath { get; set; } = "/ws";
    public int IdleTimeoutSeconds { get; set; } = 30;
    public int MaxReconnectAttempts { get; set; } = int.MaxValue;
    public int InitialBackoffMs { get; set; } = 1000;
    public int MaxBackoffMs { get; set; } = 30000;
    public IEnumerable<string> WebSocketUrls { get; set; } = new List<string>();
}