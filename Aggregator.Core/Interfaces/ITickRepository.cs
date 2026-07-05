using Aggregator.Core.Models;

namespace Aggregator.Core.Services;

/// <summary>
/// Интерфейс для хранения тиков.
/// </summary>
public interface ITickRepository
{
    /// <summary>
    /// Сохраняет тик в хранилище.
    /// </summary>
    Task SaveAsync(NormalizedTick tick, CancellationToken cancellationToken = default);

    /// <summary>
    /// Сохраняет список тиков в хранилище.
    /// </summary>
    Task SaveRangeAsync(IEnumerable<NormalizedTick> ticks, CancellationToken cancellationToken = default);

    /// <summary>
    /// Получает список тиков.
    /// </summary>
    Task<IEnumerable<NormalizedTick>> GetTicksAsync(int limit = 100, CancellationToken cancellationToken = default);
}