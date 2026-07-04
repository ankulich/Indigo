using System.Text.Json;
using Aggregator.Core.Models;

namespace Aggregator.Core.Services;

/// <summary>
/// Нормализатор тиков для Forex.
/// </summary>
public class ForexTickNormalizer : ITickNormalizer
{
    public string ExchangeName => "forex";

    public NormalizedTick Normalize(string json)
    {
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        return new NormalizedTick
        {
            Source = ExchangeName,
            Ticker = root.GetProperty("pair").GetString() ?? "UNKNOWN",
            Price = root.GetProperty("price").GetDecimal(),
            Volume = root.GetProperty("quantity").GetDecimal(),
            Timestamp = DateTimeOffset.FromUnixTimeSeconds(root.GetProperty("timestamp").GetInt64()).DateTime
        };
    }
}