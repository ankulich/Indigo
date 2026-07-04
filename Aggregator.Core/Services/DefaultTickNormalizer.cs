using System.Text.Json;
using Aggregator.Core.Models;

namespace Aggregator.Core.Services;

/// <summary>
/// Нормализатор тиков по умолчанию для неизвестных бирж.
/// </summary>
public class DefaultTickNormalizer : ITickNormalizer
{
    public string ExchangeName => "default";

    public NormalizedTick Normalize(string json)
    {
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        return new NormalizedTick
        {
            Source = "unknown",
            Ticker = root.GetProperty("symbol").GetString() ?? "UNKNOWN",
            Price = root.GetProperty("price").GetDecimal(),
            Volume = root.GetProperty("quantity").GetDecimal(),
            Timestamp = DateTimeOffset.FromUnixTimeSeconds(root.GetProperty("timestamp").GetInt64()).DateTime
        };
    }
}