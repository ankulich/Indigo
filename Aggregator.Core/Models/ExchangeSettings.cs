namespace Aggregator.Core.Models;

public class ExchangeSettings
{
    public string Name { get; set; } = string.Empty;
    public IEnumerable<string> WebSocketUrls { get; set; } = new List<string>();
}