using System.Globalization;
using System.Text.Json;
using Aggregator.Core.Models;

namespace Aggregator.Core.Services;

/// <summary>
/// Нормализатор тиков для Coinbase.
/// </summary>
public class CoinbaseTickNormalizer : ITickNormalizer
{
    public string ExchangeName => "coinbase";

    public NormalizedTick Normalize(string json)
    {
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        return new NormalizedTick
        {
            Source = ExchangeName,
            Ticker = root.GetProperty("product_id").GetString() ?? "UNKNOWN",
            Price = root.GetProperty("price").GetDecimal(),
            Volume = decimal.Parse(root.GetProperty("count").GetString() ?? "0", CultureInfo.InvariantCulture),
            Timestamp = DateTime.ParseExact(root.GetProperty("time").GetString()!, "yyyy-MM-ddTHH:mm:ss.fffZ", CultureInfo.InvariantCulture)
        };
    }
}