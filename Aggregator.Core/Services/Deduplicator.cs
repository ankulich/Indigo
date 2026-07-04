using System.Collections.Concurrent;
using Aggregator.Core.Models;

namespace Aggregator.Core.Services;

/// <summary>
/// Интерфейс для дедупликации тиков.
/// </summary>
public interface IDeduplicator
{
    /// <summary>
    /// Проверяет, является ли тик дубликатом.
    /// </summary>
    /// <param name="tick">Тик для проверки.</param>
    /// <returns>True, если тик является дубликатом.</returns>
    bool IsDuplicate(NormalizedTick tick);

    /// <summary>
    /// Очищает устаревшие записи из кэша.
    /// </summary>
    void CleanupStaleEntries();
}

/// <summary>
/// Реализация дедупликатора тиков с использованием ConcurrentDictionary.
/// Потокобезопасная реализация.
/// </summary>
public class Deduplicator : IDeduplicator
{
    private readonly ConcurrentDictionary<string, DateTime> _cache = new();
    private readonly TimeSpan _ttl;
    private readonly object _cleanupLock = new();
    private DateTime _lastCleanup = DateTime.UtcNow;
    private readonly TimeSpan _cleanupInterval = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Создает новый экземпляр Deduplicator.
    /// </summary>
    /// <param name="ttl">Время жизни записи в кэше.</param>
    public Deduplicator(TimeSpan? ttl = null)
    {
        _ttl = ttl ?? TimeSpan.FromSeconds(10);
    }

    public bool IsDuplicate(NormalizedTick tick)
    {
        if (tick == null) throw new ArgumentNullException(nameof(tick));

        // Периодически очищаем устаревшие записи
        if (DateTime.UtcNow - _lastCleanup > _cleanupInterval)
        {
            CleanupStaleEntries();
        }

        var key = CreateKey(tick);
        return !_cache.TryAdd(key, DateTime.UtcNow);
    }

    public void CleanupStaleEntries()
    {
        lock (_cleanupLock)
        {
            if (DateTime.UtcNow - _lastCleanup <= _cleanupInterval)
                return;

            var now = DateTime.UtcNow;
            var keysToRemove = new List<string>();

            foreach (var kvp in _cache)
            {
                if (now - kvp.Value > _ttl)
                {
                    keysToRemove.Add(kvp.Key);
                }
            }

            foreach (var key in keysToRemove)
            {
                _cache.TryRemove(key, out _);
            }

            _lastCleanup = now;
        }
    }

    private static string CreateKey(NormalizedTick tick)
    {
        return $"{tick.Source}_{tick.Ticker}_{tick.Timestamp:O}_{tick.Price}_{tick.Volume}";
    }
}
