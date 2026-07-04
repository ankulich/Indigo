using Aggregator.Core.Models;

namespace Aggregator.Core.Services;

/// <summary>
/// Интерфейс для нормализации тиков с различных бирж.
/// </summary>
public interface ITickNormalizer
{
    /// <summary>
    /// Имя биржи, которую обрабатывает этот нормализатор.
    /// </summary>
    string ExchangeName { get; }

    /// <summary>
    /// Нормализует JSON-данные в стандартный формат тика.
    /// </summary>
    /// <param name="json">JSON-строка с данными тика.</param>
    /// <returns>Нормализованный тик.</returns>
    NormalizedTick Normalize(string json);
}