using Aggregator.Core.Models;
using System.Collections.Concurrent;

namespace Aggregator.Core.Services;

/// <summary>
/// Фабрика для получения нормализаторов тиков по имени биржи.
/// </summary>
public class TickNormalizerFactory
{
    private readonly ConcurrentDictionary<string, ITickNormalizer> _normalizers = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Регистрирует нормализатор для указанной биржи.
    /// </summary>
    public void Register(ITickNormalizer normalizer)
    {
        if (normalizer == null) throw new ArgumentNullException(nameof(normalizer));

        _normalizers.TryAdd(normalizer.ExchangeName, normalizer);
    }

    /// <summary>
    /// Получает нормализатор для указанной биржи.
    /// </summary>
    /// <param name="exchangeName">Имя биржи.</param>
    /// <returns>Нормализатор тиков.</returns>
    /// <exception cref="KeyNotFoundException">Если нормализатор не найден.</exception>
    public ITickNormalizer GetNormalizer(string exchangeName)
    {
        if (string.IsNullOrEmpty(exchangeName))
            throw new ArgumentException("Exchange name cannot be null or empty.", nameof(exchangeName));

        if (!_normalizers.TryGetValue(exchangeName, out var normalizer))
            throw new KeyNotFoundException($"Normalizer for exchange '{exchangeName}' not found.");

        return normalizer;
    }
}