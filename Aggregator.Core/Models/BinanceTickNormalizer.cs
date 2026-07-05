using System.Text.Json;
using Aggregator.Core.Models;

namespace Aggregator.Core.Services;

/// <summary>
/// Нормализатор тиков для Binance.
/// </summary>
public class BinanceTickNormalizer : ITickNormalizer
{
    public string ExchangeName => "binance";

    public NormalizedTick Normalize(string json)
    {
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        return new NormalizedTick
        {
            Source = ExchangeName,
            Ticker = root.GetProperty("s").GetString() ?? "UNKNOWN",
            Price = decimal.Parse(root.GetProperty("p").GetString() ?? "0", System.Globalization.CultureInfo.InvariantCulture),
            Volume = root.GetProperty("q").GetDecimal(),
            Timestamp = DateTimeOffset.FromUnixTimeMilliseconds(root.GetProperty("T").GetInt64()).DateTime
        };
    }
}