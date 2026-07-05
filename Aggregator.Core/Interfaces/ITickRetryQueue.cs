using Aggregator.Core.Models;

namespace Aggregator.Core.Services;

/// <summary>
/// Интерфейс для очереди повторных попыток записи тиков.
/// </summary>
public interface ITickRetryQueue
{
    /// <summary>
    /// Добавляет тик в очередь повторных попыток.
    /// </summary>
    void Enqueue(NormalizedTick tick);

    /// <summary>
    /// Текущее количество тиков в очереди.
    /// </summary>
    int Count { get; }
}