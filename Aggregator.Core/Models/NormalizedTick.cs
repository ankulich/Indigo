namespace Aggregator.Core.Models;

public record NormalizedTick
{
    public string Source { get; init; } = string.Empty;
    public string Ticker { get; init; } = string.Empty;
    public decimal Price { get; init; }
    public decimal Volume { get; init; }
    public DateTime Timestamp { get; init; }
}